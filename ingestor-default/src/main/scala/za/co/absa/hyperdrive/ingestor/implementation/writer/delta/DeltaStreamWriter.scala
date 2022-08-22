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

package za.co.absa.hyperdrive.ingestor.implementation.writer.delta

import io.delta.tables.DeltaTable
import org.apache.commons.configuration2.Configuration
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Row, functions}
import org.slf4j.LoggerFactory
import za.co.absa.hyperdrive.ingestor.api.utils.{ConfigUtils, StreamWriterUtil}
import za.co.absa.hyperdrive.ingestor.api.writer.{StreamWriter, StreamWriterFactory, StreamWriterProperties}

private[writer] class DeltaStreamWriter(destination: String,
                                        trigger: Trigger,
                                        checkpointLocation: String,
                                        partitionColumns: Seq[String],
                                        keyColumn: String,
                                        timestampColumn: String,
                                        opColumn: String,
                                        deletedValue: String,
                                        val extraConfOptions: Map[String, String]) extends StreamWriter {
  private val logger = LoggerFactory.getLogger(this.getClass)
  if (StringUtils.isBlank(destination)) {
    throw new IllegalArgumentException(s"Invalid DELTA destination: '$destination'")
  }

  override def write(dataFrame: DataFrame): StreamingQuery = {
    dataFrame.writeStream
      .trigger(trigger)
      .outputMode(OutputMode.Append())
      .option(StreamWriterProperties.CheckpointLocation, checkpointLocation)
      .options(extraConfOptions)
      .foreachBatch((df: DataFrame, batchId: Long) => {
        if(!DeltaTable.isDeltaTable(df.sparkSession, destination)) {
          df.sparkSession
            .createDataFrame(df.sparkSession.sparkContext.emptyRDD[Row], df.schema)
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy(partitionColumns :_*)
            .save(destination)
        }

        logger.info(s"Writing batchId: $batchId")

        val fieldNames = df.schema.fieldNames.filter(_ != s"$timestampColumn").mkString(",")
        val latestChangeForEachKey = df
          .selectExpr(s"$keyColumn", s"struct($timestampColumn, $fieldNames) as otherCols" )
          .groupBy(s"$keyColumn")
          .agg(functions.max("otherCols").as("latest"))
          .selectExpr( "latest.*")


        DeltaTable
          .forPath(destination)
          .as("t")
          .merge(latestChangeForEachKey.as("s"), s"s.$keyColumn = t.$keyColumn")
          .whenMatched(s"s.$opColumn = '$deletedValue'")
          .delete()
          .whenMatched()
          .updateAll()
          .whenNotMatched(s"s.$opColumn != '$deletedValue'")
          .insertAll()
          .execute()

      })
      .start()
  }

  def getDestination: String = destination
}

object DeltaStreamWriter extends StreamWriterFactory with DeltaStreamWriterAttributes {

  def apply(config: Configuration): StreamWriter = {
    val destinationDirectory = getDestinationDirectory(config)
    val trigger = StreamWriterUtil.getTrigger(config)
    val checkpointLocation = StreamWriterUtil.getCheckpointLocation(config)
    val extraOptions = getExtraOptions(config)
    val partitionColumns = ConfigUtils.getSeqOrNone(KEY_PARTITION_COLUMNS, config).getOrElse(Seq())

    val keyColumn = ConfigUtils.getOrThrow(KEY_KEY_COLUMN, config)
    val timestampColumn = ConfigUtils.getOrThrow(KEY_TIMESTAMP_COLUMN, config)
    val opColumn = ConfigUtils.getOrThrow(KEY_OP_COLUMN, config)
    val deletedValue = ConfigUtils.getOrThrow(KEY_OP_DELETED_VALUE, config)

    LoggerFactory.getLogger(this.getClass).info(s"Going to create DeltaStreamWriter instance using: " +
      s"destination directory='$destinationDirectory', trigger='$trigger', checkpointLocation='$checkpointLocation', extra options='$extraOptions'")

    new DeltaStreamWriter(destinationDirectory, trigger, checkpointLocation, partitionColumns, keyColumn, timestampColumn, opColumn, deletedValue, extraOptions)
  }

  def getDestinationDirectory(configuration: Configuration): String = ConfigUtils.getOrThrow(KEY_DESTINATION_DIRECTORY, configuration, errorMessage = s"Destination directory not found. Is '$KEY_DESTINATION_DIRECTORY' defined?")

  def getExtraOptions(configuration: Configuration): Map[String, String] = ConfigUtils.getPropertySubset(configuration, KEY_EXTRA_CONFS_ROOT)

  override def getExtraConfigurationPrefix: Option[String] = Some(KEY_EXTRA_CONFS_ROOT)
}
