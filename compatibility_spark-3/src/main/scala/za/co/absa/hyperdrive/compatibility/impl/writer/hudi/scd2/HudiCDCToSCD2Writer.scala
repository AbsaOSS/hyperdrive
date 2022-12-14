/*
 * Copyright 2018 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.hyperdrive.compatibility.impl.writer.hudi.scd2

import org.apache.commons.configuration2.Configuration
import org.apache.commons.lang3.StringUtils
import org.apache.hudi.DataSourceWriteOptions.{HIVE_STYLE_PARTITIONING, PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD, TABLE_NAME}
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.hudi.exception.TableNotFoundException
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.types.{BooleanType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, DataFrameWriter, Row, SaveMode, functions}
import org.slf4j.LoggerFactory
import za.co.absa.hyperdrive.compatibility.impl.writer.hudi.HudiUtil
import za.co.absa.hyperdrive.compatibility.impl.writer.hudi.scd2.HudiCDCToSCD2Writer.logger
import za.co.absa.hyperdrive.ingestor.api.utils.{ConfigUtils, StreamWriterUtil}
import za.co.absa.hyperdrive.ingestor.api.writer.{StreamWriter, StreamWriterFactory}

import java.io.FileNotFoundException

private[writer] class HudiCDCToSCD2Writer(destination: String,
                                          trigger: Trigger,
                                          checkpointLocation: String,
                                          partitionColumns: Seq[String],
                                          keyColumn: String,
                                          timestampColumn: String,
                                          operationColumn: String,
                                          operationDeleteValues: Seq[String],
                                          precombineColumns: Seq[String],
                                          precombineColumnsCustomOrder: Map[String, Seq[String]],
                                          val extraConfOptions: Map[String, String]) extends StreamWriter {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private val CheckpointLocation = "checkpointLocation"

  private val StringSeparator = "#$@"
  private val StartDateColumn = "_start_date"
  private val EndDateColumn = "_end_date"
  private val IsCurrentColumn = "_is_current"
  private val IsOldDataColumn = "_is_old_data"
  private val SortFieldPrefix = "_tmp_hyperdrive_"
  private val OldData = "_old_data"
  private val NewData = "_new_data"

  if (StringUtils.isBlank(destination)) {
    throw new IllegalArgumentException("Destination must not be blank!")
  }
  if (precombineColumnsCustomOrder.values.flatten.toSeq.contains(StringSeparator)) {
    throw new IllegalArgumentException(s"Precombine columns custom order cannot contain string separator: $StringSeparator")
  }

  override def write(dataFrame: DataFrame): StreamingQuery = {
    dataFrame.writeStream
      .trigger(trigger)
      .outputMode(OutputMode.Append())
      .option(CheckpointLocation, checkpointLocation)
      .options(extraConfOptions)
      .foreachBatch((df: DataFrame, batchId: Long) => {
        logger.info(s"Writing batchId: $batchId")
        val spark = df.sparkSession
        val dataFrameSchema = StructType(
          Seq(
            StructField(StartDateColumn, TimestampType, nullable = false),
            StructField(EndDateColumn, TimestampType, nullable = true),
            StructField(IsCurrentColumn, BooleanType, nullable = false)
          ).toArray ++ dataFrame.schema.fields
        )

        var hudiTable: DataFrame = null
        try {
          hudiTable = df.sparkSession.read.format("hudi").load(destination)
            .drop("_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key", "_hoodie_partition_path", "_hoodie_file_name")
        } catch {
          case e: TableNotFoundException => {
            logger.info(s"EMPTY HUDI TABLE")
            hudiTable = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], dataFrameSchema)
          }
          case e: FileNotFoundException => {
            logger.info(s"EMPTY HUDI TABLE")
            hudiTable = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], dataFrameSchema)
          }
        }

        val uniqueChangesForEachKeyAndTimestamp = removeDuplicates(df)
        val previousEvents = getPreviousEvents(hudiTable, uniqueChangesForEachKeyAndTimestamp)
        val nextEvents = getNextEvents(hudiTable, uniqueChangesForEachKeyAndTimestamp)

        val union = previousEvents.union(nextEvents).distinct().union(
          uniqueChangesForEachKeyAndTimestamp
            .withColumn(StartDateColumn, col(timestampColumn))
            .withColumn(EndDateColumn, lit(null))
            .withColumn(IsCurrentColumn, lit(false))
            .withColumn(IsOldDataColumn, lit(false))
            .selectExpr(
              Seq(StartDateColumn, EndDateColumn, IsCurrentColumn) ++
                uniqueChangesForEachKeyAndTimestamp.columns ++
                Seq(IsOldDataColumn): _*
            )
        )

        val uniqueEvents = removeDuplicates(union)
        val stagedData = setSCD2Fields(uniqueEvents).drop(IsOldDataColumn)
        generateDataFrameWriter(stagedData).save(destination)
      })
      .start()
  }

  private def getPreviousEvents(hudiTable: DataFrame, uniqueChangesForEachKeyAndTimestamp: DataFrame): DataFrame = {
    hudiTable.as(OldData).join(
      uniqueChangesForEachKeyAndTimestamp.as(NewData),
      col(s"$NewData.$keyColumn").equalTo(col(s"$OldData.$keyColumn"))
        .and(col(s"$NewData.$timestampColumn").>=(col(s"$OldData.$StartDateColumn")))
        .and(col(s"$NewData.$timestampColumn").<=(col(s"$OldData.$EndDateColumn")))
        .or(
          col(s"$NewData.$keyColumn").equalTo(col(s"$OldData.$keyColumn"))
            .and(col(s"$NewData.$timestampColumn").>=(col(s"$OldData.$StartDateColumn")))
            .and(col(s"$OldData.$IsCurrentColumn").equalTo(true))
        )
    ).select(s"$OldData.*").withColumn(s"$IsOldDataColumn", lit(true))
  }

  private def getNextEvents(hudiTable: DataFrame, uniqueChangesForEachKeyAndTimestamp: DataFrame): DataFrame = {
    val fieldNames = hudiTable.schema.fieldNames
      .filter(_ != StartDateColumn)
      .filter(_ != timestampColumn)
    val originalFieldNames = hudiTable.schema.fieldNames

    val window = Window
      .partitionBy(col(s"$keyColumn").asc, col(s"$NewData.$timestampColumn").asc)
      .orderBy(s"otherCols.$StartDateColumn", s"otherCols.$timestampColumn")

    hudiTable.as(OldData).join(
      uniqueChangesForEachKeyAndTimestamp.as(NewData),
      col(s"$NewData.$keyColumn").equalTo(col(s"$OldData.$keyColumn"))
        .and(col(s"$NewData.$timestampColumn").<(col(s"$OldData.$StartDateColumn")))
    ).select(s"$OldData.*", s"$NewData.$timestampColumn")
      .selectExpr(
        s"$keyColumn",
        s"$NewData.$timestampColumn",
        s"struct($StartDateColumn, $OldData.$timestampColumn, ${fieldNames.mkString(",")}) as otherCols"
      )
      .withColumn("rank", functions.row_number().over(window))
      .where("rank == 1")
      .drop("rank")

      .selectExpr("otherCols.*")
      .select(originalFieldNames.head, originalFieldNames.tail: _*)
      .withColumn(s"$IsOldDataColumn", lit(true))
  }

  private def generateDataFrameWriter(latestChanges: DataFrame): DataFrameWriter[Row] = {
    latestChanges.write
      .format("hudi")
      .options(getQuickstartWriteConfigs)
      .option(PRECOMBINE_FIELD.key(), timestampColumn)
      .option(RECORDKEY_FIELD.key(), s"$keyColumn,$timestampColumn")
      .option(PARTITIONPATH_FIELD.key(), partitionColumns.mkString(","))
      .option(HIVE_STYLE_PARTITIONING.key(), "true")
      .option(TABLE_NAME.key(), destination)
      .option("hoodie.table.name", destination)
      .mode(SaveMode.Append)
      .options(extraConfOptions)
  }

  private def setSCD2Fields(dataFrame: DataFrame): DataFrame = {
    val idWindowDesc = org.apache.spark.sql.expressions.Window
      .partitionBy(keyColumn)
      .orderBy(col(timestampColumn).desc, col(IsOldDataColumn).desc)
    dataFrame
      .withColumn(
        EndDateColumn,
        when(
          col(IsOldDataColumn).equalTo(true).and(
            lag(keyColumn, 1, null).over(idWindowDesc).isNull
          ),
          col(EndDateColumn)
        ).when(
          col(IsOldDataColumn).equalTo(true).and(
            lag(IsOldDataColumn, 1, false).over(idWindowDesc).equalTo(true)
          ),
          col(EndDateColumn)
        ).otherwise(
          lag(StartDateColumn, 1, null).over(idWindowDesc)
        )
      )
      .withColumn(
        EndDateColumn,
        when(col(operationColumn).isInCollection(operationDeleteValues), col(StartDateColumn))
          .when(!col(operationColumn).isInCollection(operationDeleteValues), col(EndDateColumn))
          .otherwise(null)
      )
      .withColumn(
        IsCurrentColumn,
        when(col(EndDateColumn).isNull, lit(true)).otherwise(lit(false))
      )
  }

  private def removeDuplicates(inputDF: DataFrame): DataFrame = {
    val dataFrameWithSortColumns = HudiUtil.getDataFrameWithSortColumns(inputDF, SortFieldPrefix, precombineColumns, precombineColumnsCustomOrder)
    val sortColumnsWithPrefix = dataFrameWithSortColumns.schema.fieldNames.filter(_.startsWith(SortFieldPrefix))
    val window = Window
      .partitionBy(s"$keyColumn", s"$timestampColumn")
      .orderBy(sortColumnsWithPrefix.map(col(_).desc): _*)
    dataFrameWithSortColumns
      .withColumn("rank", functions.row_number().over(window))
      .where("rank == 1")
      .drop("rank")
      .drop(sortColumnsWithPrefix: _*)
  }
}

object HudiCDCToSCD2Writer extends StreamWriterFactory with HudiCDCToSCD2WriterAttributes {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def apply(config: Configuration): StreamWriter = {
    val destinationDirectory = getDestinationDirectory(config)
    val trigger = StreamWriterUtil.getTrigger(config)
    val checkpointLocation = StreamWriterUtil.getCheckpointLocation(config)
    val extraOptions = getExtraOptions(config)
    val partitionColumns = ConfigUtils.getSeqOrNone(KEY_PARTITION_COLUMNS, config).getOrElse(Seq())

    val keyColumn = ConfigUtils.getOrThrow(KEY_KEY_COLUMN, config)
    val timestampColumn = ConfigUtils.getOrThrow(KEY_TIMESTAMP_COLUMN, config)
    val operationColumn = ConfigUtils.getOrThrow(KEY_OPERATION_COLUMN, config)
    val operationDeleteValues = ConfigUtils.getSeqOrThrow(KEY_OPERATION_DELETED_VALUES, config)
    val precombineColumns = ConfigUtils.getSeqOrThrow(KEY_PRECOMBINE_COLUMNS, config)
    val precombineColumnsCustomOrder = ConfigUtils.getMapOrEmpty(KEY_PRECOMBINE_COLUMNS_CUSTOM_ORDER, config)


    logger.info(s"Going to create HudiStreamWriter instance using: " +
      s"destination directory='$destinationDirectory', trigger='$trigger', checkpointLocation='$checkpointLocation', " +
      s"partition columns='$partitionColumns', key column='$keyColumn', timestamp column='$timestampColumn', operation column='$operationColumn', " +
      s"operation delete values='$operationDeleteValues', precombine columns='$precombineColumns', " +
      s"precombine columns custom order='$precombineColumnsCustomOrder', extra options='$extraOptions'")

    new HudiCDCToSCD2Writer(destinationDirectory, trigger, checkpointLocation, partitionColumns, keyColumn, timestampColumn, operationColumn, operationDeleteValues, precombineColumns, precombineColumnsCustomOrder, extraOptions)
  }

  def getDestinationDirectory(configuration: Configuration): String = ConfigUtils.getOrThrow(KEY_DESTINATION_DIRECTORY, configuration, errorMessage = s"Destination directory not found. Is '$KEY_DESTINATION_DIRECTORY' defined?")

  def getExtraOptions(configuration: Configuration): Map[String, String] = ConfigUtils.getPropertySubset(configuration, KEY_EXTRA_CONFS_ROOT)

  override def getExtraConfigurationPrefix: Option[String] = Some(KEY_EXTRA_CONFS_ROOT)
}
