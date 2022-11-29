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

package za.co.absa.hyperdrive.compatibility.impl.writer.delta.simple

import io.delta.tables.DeltaTable
import org.apache.commons.configuration2.Configuration
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, when}
import org.slf4j.LoggerFactory
import za.co.absa.hyperdrive.compatibility.impl.writer.delta.DeltaUtil
import za.co.absa.hyperdrive.ingestor.api.utils.{ConfigUtils, StreamWriterUtil}
import za.co.absa.hyperdrive.ingestor.api.writer.{StreamWriter, StreamWriterFactory, StreamWriterProperties}

private[writer] class DeltaSimpleWriter(destination: String, trigger: Trigger,
                                        checkpointLocation: String,
                                        partitionColumns: Option[Seq[String]],
                                        keyColumn: String,
                                        operationColumn: String,
                                        operationDeleteValues: Seq[String],
                                        val extraConfOptions: Map[String, String]) extends StreamWriter {

  private val logger = LoggerFactory.getLogger(this.getClass)
  if (StringUtils.isBlank(destination)) {
    throw new IllegalArgumentException(s"Invalid PARQUET destination: '$destination'")
  }

  override def write(dataFrame: DataFrame): StreamingQuery = {
    DeltaUtil.createDeltaTableIfNotExists(dataFrame.sparkSession, destination, dataFrame.schema, partitionColumns.getOrElse(Seq()))

    logger.info(s"Writing to $destination")
    val dataStreamWriter = dataFrame.writeStream
    val dataStreamWriterWithPartition = partitionColumns match {
      case Some(columns) => dataStreamWriter.partitionBy(columns: _*)
      case None => dataStreamWriter
    }
    dataStreamWriterWithPartition
      .trigger(trigger)
      .format(source = "parquet")
      .outputMode(OutputMode.Append())
      .option(StreamWriterProperties.CheckpointLocation, checkpointLocation)
      .options(extraConfOptions)
      .foreachBatch((df: DataFrame, batchId: Long) => {
        DeltaTable
          .forPath(destination)
          .as("currentTable")
          .merge(df.as("changes"), s"currentTable.$keyColumn = changes.$keyColumn")
          .whenMatched(when(col(s"changes.$operationColumn").isInCollection(operationDeleteValues), true).otherwise(false))
          .delete()
          .whenMatched(when(col(s"changes.$operationColumn").isInCollection(operationDeleteValues), false).otherwise(true))
          .updateAll()
          .whenNotMatched(when(col(s"changes.$operationColumn").isInCollection(operationDeleteValues), false).otherwise(true))
          .insertAll()
          .execute()
      }).start()
  }

  def getDestination: String = destination
}

object DeltaSimpleWriter extends StreamWriterFactory with DeltaSimpleWriterAttributes {
  def apply(config: Configuration): StreamWriter = {
    val destinationDirectory = getDestinationDirectory(config)
    val trigger = StreamWriterUtil.getTrigger(config)
    val checkpointLocation = StreamWriterUtil.getCheckpointLocation(config)
    val partitionColumns = ConfigUtils.getSeqOrNone(KEY_PARTITION_COLUMNS, config)
    val extraOptions = getExtraOptions(config)
    val keyColumn = ConfigUtils.getOrThrow(KEY_KEY_COLUMN, config)
    val operationColumn = ConfigUtils.getOrThrow(KEY_OPERATION_COLUMN, config)
    val operationDeleteValues = ConfigUtils.getSeqOrThrow(KEY_OPERATION_DELETED_VALUES, config)

    LoggerFactory.getLogger(this.getClass).info(s"Going to create ParquetStreamWriter instance using: " +
      s"destination directory='$destinationDirectory', trigger='$trigger', checkpointLocation='$checkpointLocation', extra options='$extraOptions'")

    new DeltaSimpleWriter(destinationDirectory, trigger, checkpointLocation, partitionColumns, keyColumn, operationColumn, operationDeleteValues, extraOptions)
  }

  def getDestinationDirectory(configuration: Configuration): String = ConfigUtils.getOrThrow(KEY_DESTINATION_DIRECTORY, configuration, errorMessage = s"Destination directory not found. Is '$KEY_DESTINATION_DIRECTORY' defined?")

  def getExtraOptions(configuration: Configuration): Map[String, String] = ConfigUtils.getPropertySubset(configuration, KEY_EXTRA_CONFS_ROOT)

  override def getExtraConfigurationPrefix: Option[String] = Some(KEY_EXTRA_CONFS_ROOT)
}
