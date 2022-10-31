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

package za.co.absa.hyperdrive.compatibility.impl.writer.delta.snapshot

import io.delta.tables.{DeltaMergeBuilder, DeltaTable}
import org.apache.commons.configuration2.Configuration
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.{Column, DataFrame, Row, SaveMode, SparkSession, functions}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.slf4j.LoggerFactory
import za.co.absa.hyperdrive.ingestor.api.utils.{ConfigUtils, StreamWriterUtil}
import za.co.absa.hyperdrive.ingestor.api.writer.{StreamWriter, StreamWriterFactory}
import za.co.absa.hyperdrive.shared.utils.FileUtils

import java.net.URI

private[writer] class DeltaCDCToSnapshotWriter(destination: String,
                                               trigger: Trigger,
                                               checkpointLocation: String,
                                               partitionColumns: Seq[String],
                                               keyColumn: String,
                                               operationColumn: String,
                                               operationDeleteValues: Seq[String],
                                               precombineColumns: Seq[String],
                                               precombineColumnsCustomOrder: Map[String, Seq[String]],
                                               val extraConfOptions: Map[String, String]) extends StreamWriter {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val STRING_SEPARATOR = "#$@"
  private val CHECKPOINT_LOCATION = "checkpointLocation"

  if (StringUtils.isBlank(destination)) {
    throw new IllegalArgumentException("Destination must not be blank!")
  }
  if (precombineColumnsCustomOrder.values.flatten.toSeq.contains(STRING_SEPARATOR)) {
    throw new IllegalArgumentException(s"Precombine columns custom order cannot contain string separator: $STRING_SEPARATOR")
  }

  override def write(dataFrame: DataFrame): StreamingQuery = {
    if (!DeltaTable.isDeltaTable(dataFrame.sparkSession, destination)) {
      if (isDirEmptyOrDoesNotExist(dataFrame.sparkSession, destination)) {
        logger.info(s"Destination: $destination is not a delta table. Creating new delta table.")
        dataFrame.sparkSession
          .createDataFrame(dataFrame.sparkSession.sparkContext.emptyRDD[Row], dataFrame.schema)
          .write
          .format("delta")
          .mode(SaveMode.Overwrite)
          .option("overwriteSchema", "true")
          .partitionBy(partitionColumns: _*)
          .save(destination)
      } else {
        throw new IllegalArgumentException(s"Could not create new delta table. Directory $destination is not empty!")
      }
    }

    dataFrame.writeStream
      .trigger(trigger)
      .outputMode(OutputMode.Append())
      .option(CHECKPOINT_LOCATION, checkpointLocation)
      .options(extraConfOptions)
      .foreachBatch((df: DataFrame, batchId: Long) => {
        logger.info(s"Writing batchId: $batchId")

        val sortFieldsPrefix = "_tmp_hyperdrive_"

        val dataFrameWithSortColumns = getDataFrameWithSortColumns(df, sortFieldsPrefix)

        val originalFieldNames = df.schema.fieldNames.mkString(",")
        val sortColumnsWithPrefix = precombineColumns.map(precombineColumn => s"$sortFieldsPrefix$precombineColumn")

        val latestChangeForEachKey = dataFrameWithSortColumns
          .selectExpr(s"$keyColumn", s"struct(${sortColumnsWithPrefix.mkString(",")}, $originalFieldNames) as otherCols")
          .groupBy(s"$keyColumn")
          .agg(functions.max("otherCols").as("latest"))
          .filter(col("latest").isNotNull)
          .withColumn("latest", new Column(AssertNotNull(col("latest").expr)))
          .selectExpr("latest.*")
          .drop(sortColumnsWithPrefix: _*)

        generateDeltaMerge(latestChangeForEachKey).execute()
      })
      .start()
  }

  private def generateDeltaMerge(latestChanges: DataFrame): DeltaMergeBuilder = {
    val initialDeltaBuilder = DeltaTable
      .forPath(destination)
      .as("currentTable")
      .merge(latestChanges.as("changes"), s"currentTable.$keyColumn = changes.$keyColumn")
      .whenMatched(when(col(s"changes.$operationColumn").isInCollection(operationDeleteValues), true).otherwise(false))
      .delete()

    val deltaBuilderWithSortColumns = precombineColumns.foldLeft(initialDeltaBuilder) { (builder, precombineColumn) =>
      val order = precombineColumnsCustomOrder.getOrElse(precombineColumn, Seq.empty[String])
      order match {
        case o if o.isEmpty =>
          builder
            .whenMatched(s"changes.$precombineColumn > currentTable.$precombineColumn")
            .updateAll()
        case o =>
          val orderString = o.mkString(STRING_SEPARATOR)
          builder
            .whenMatched(s"""locate(changes.$precombineColumn, "$orderString") > locate(currentTable.$precombineColumn, "$orderString")""")
            .updateAll()
      }
    }

    deltaBuilderWithSortColumns
      .whenNotMatched(when(col(s"changes.$operationColumn").isInCollection(operationDeleteValues), false).otherwise(true))
      .insertAll()
  }

  private def getDataFrameWithSortColumns(dataFrame: DataFrame, sortFieldsPrefix: String): DataFrame = {
    precombineColumns.foldLeft(dataFrame) { (df, precombineColumn) =>
      val order = precombineColumnsCustomOrder.getOrElse(precombineColumn, Seq.empty[String])
      order match {
        case o if o.isEmpty =>
          df.withColumn(s"$sortFieldsPrefix$precombineColumn", col(precombineColumn))
        case o =>
          val orderString = o.mkString(STRING_SEPARATOR)
          df.withColumn(s"$sortFieldsPrefix$precombineColumn", functions.expr(s"""locate($precombineColumn, "$orderString")"""))
      }
    }
  }

  private def isDirEmptyOrDoesNotExist(spark: SparkSession, destination: String): Boolean = {
    implicit val fs: FileSystem = FileSystem.get(new URI(destination), spark.sparkContext.hadoopConfiguration)
    if (FileUtils.exists(destination)) {
      if (FileUtils.isDirectory(destination)) {
        FileUtils.isEmpty(destination)
      } else {
        false
      }
    } else {
      true
    }
  }
}

object DeltaCDCToSnapshotWriter extends StreamWriterFactory with DeltaCDCToSnapshotWriterAttributes {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def apply(config: Configuration): StreamWriter = {
    val destinationDirectory = getDestinationDirectory(config)
    val trigger = StreamWriterUtil.getTrigger(config)
    val checkpointLocation = StreamWriterUtil.getCheckpointLocation(config)
    val extraOptions = getExtraOptions(config)
    val partitionColumns = ConfigUtils.getSeqOrNone(KEY_PARTITION_COLUMNS, config).getOrElse(Seq())

    val keyColumn = ConfigUtils.getOrThrow(KEY_KEY_COLUMN, config)
    val operationColumn = ConfigUtils.getOrThrow(KEY_OPERATION_COLUMN, config)
    val operationDeleteValues = ConfigUtils.getSeqOrThrow(KEY_OPERATION_DELETED_VALUES, config)
    val precombineColumns = ConfigUtils.getSeqOrThrow(KEY_PRECOMBINE_COLUMNS, config)
    val precombineColumnsCustomOrder = ConfigUtils.getMapOrEmpty(KEY_PRECOMBINE_COLUMNS_CUSTOM_ORDER, config)

    logger.info(s"Going to create DeltaStreamWriter instance using: " +
      s"destination directory='$destinationDirectory', trigger='$trigger', checkpointLocation='$checkpointLocation', " +
      s"partition columns='$partitionColumns', key column='$keyColumn', operation column='$operationColumn', " +
      s"operation delete values='$operationDeleteValues', precombine columns='$precombineColumns', " +
      s"precombine columns custom order='$precombineColumnsCustomOrder', extra options='$extraOptions'")

    new DeltaCDCToSnapshotWriter(destinationDirectory, trigger, checkpointLocation, partitionColumns, keyColumn, operationColumn, operationDeleteValues, precombineColumns, precombineColumnsCustomOrder, extraOptions)
  }

  def getDestinationDirectory(configuration: Configuration): String = ConfigUtils.getOrThrow(KEY_DESTINATION_DIRECTORY, configuration, errorMessage = s"Destination directory not found. Is '$KEY_DESTINATION_DIRECTORY' defined?")

  def getExtraOptions(configuration: Configuration): Map[String, String] = ConfigUtils.getPropertySubset(configuration, KEY_EXTRA_CONFS_ROOT)

  override def getExtraConfigurationPrefix: Option[String] = Some(KEY_EXTRA_CONFS_ROOT)
}
