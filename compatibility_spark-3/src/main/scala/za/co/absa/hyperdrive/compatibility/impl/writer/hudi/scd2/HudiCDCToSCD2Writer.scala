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
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.types.{BooleanType, StructField, TimestampType}
import org.apache.spark.sql.{Column, DataFrame}
import org.slf4j.LoggerFactory
import za.co.absa.hyperdrive.compatibility.impl.writer.hudi.HudiUtil
import za.co.absa.hyperdrive.ingestor.api.utils.{ConfigUtils, StreamWriterUtil}
import za.co.absa.hyperdrive.ingestor.api.writer.{StreamWriter, StreamWriterFactory}

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
    val dataFrameSchema = Seq(
      StructField(StartDateColumn, TimestampType, nullable = false),
      StructField(EndDateColumn, TimestampType, nullable = true),
      StructField(IsCurrentColumn, BooleanType, nullable = false)
    ).toArray ++ dataFrame.schema.fields


    HudiUtil.createHudiTableIfNotExists(dataFrame.sparkSession, destination, dataFrameSchema, keyColumn, timestampColumn)

    dataFrame.writeStream
      .trigger(trigger)
      .outputMode(OutputMode.Append())
      .option(CheckpointLocation, checkpointLocation)
      .options(extraConfOptions)
      .foreachBatch((df: DataFrame, batchId: Long) => {
        logger.info(s"Writing batchId: $batchId")
        val hudiTable = df.sparkSession.read.format("hudi").load(destination)

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
        df.sparkSession.sql(generateHudiMerge(stagedData))
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
      .groupBy(s"$keyColumn", s"$NewData.$timestampColumn")
      .agg(min("otherCols").as("latest"))
      .filter(col("latest").isNotNull)
      .withColumn("latest", new Column(AssertNotNull(col("latest").expr)))
      .selectExpr("latest.*")
      .select(originalFieldNames.head, originalFieldNames.tail: _*)
      .withColumn(s"$IsOldDataColumn", lit(true))
  }

  private def generateHudiMerge(latestChanges: DataFrame): String = {
    latestChanges.sparkSession.sql(
      s"""
         |create table historical_data using hudi
         |location '$destination';
         |""".stripMargin)

    s"""
       | merge into historical_data as target
       | using (
       |   select * from latestChanges
       | ) source
       | on  target.$keyColumn = source.$keyColumn AND target.$StartDateColumn = source.$StartDateColumn
       | when matched then
       |   update set *
       | when not matched then insert *
         """.stripMargin
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

    val originalFieldNames = inputDF.schema.fieldNames.mkString(",")
    val sortColumnsWithPrefix = dataFrameWithSortColumns.schema.fieldNames.filter(_.startsWith(SortFieldPrefix))

    dataFrameWithSortColumns
      .selectExpr(
        s"$keyColumn",
        s"$timestampColumn",
        s"struct(${sortColumnsWithPrefix.mkString(",")}, $originalFieldNames) as otherCols"
      )
      .groupBy(s"$keyColumn", s"$timestampColumn")
      .agg(max("otherCols").as("latest"))
      .filter(col("latest").isNotNull)
      .withColumn("latest", new Column(AssertNotNull(col("latest").expr)))
      .selectExpr("latest.*")
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
