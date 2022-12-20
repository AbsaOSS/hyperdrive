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

package za.co.absa.hyperdrive.compatibility.impl.writer.cdc.delta.scd2

import io.delta.tables.{DeltaMergeBuilder, DeltaTable}
import org.apache.commons.configuration2.Configuration
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.slf4j.LoggerFactory
import za.co.absa.hyperdrive.compatibility.impl.writer.cdc.CDCUtil.{SCD2Fields, getSchemaWithSCD2Fields, getStagedDataForSCD2}
import za.co.absa.hyperdrive.compatibility.impl.writer.cdc.delta.DeltaUtil
import za.co.absa.hyperdrive.ingestor.api.utils.{ConfigUtils, StreamWriterUtil}
import za.co.absa.hyperdrive.ingestor.api.writer.{StreamWriter, StreamWriterFactory}

private[writer] class DeltaCDCToSCD2Writer(destination: String,
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

  if (StringUtils.isBlank(destination)) {
    throw new IllegalArgumentException("Destination must not be blank!")
  }

  override def write(dataFrame: DataFrame): StreamingQuery = {
    DeltaUtil.createDeltaTableIfNotExists(dataFrame.sparkSession, destination, getSchemaWithSCD2Fields(dataFrame), partitionColumns)

    dataFrame.writeStream
      .trigger(trigger)
      .outputMode(OutputMode.Append())
      .option(CheckpointLocation, checkpointLocation)
      .options(extraConfOptions)
      .foreachBatch((input: DataFrame, batchId: Long) => {
        logger.info(s"Writing batchId: $batchId")

        val deltaTable = DeltaTable.forPath(destination).toDF
        val scd2Fields = SCD2Fields(keyColumn, timestampColumn, operationColumn, operationDeleteValues, precombineColumns, precombineColumnsCustomOrder)
        val stagedData = getStagedDataForSCD2(deltaTable, input, scd2Fields)
        generateDeltaMerge(stagedData).execute()
      })
      .start()
  }

  private def generateDeltaMerge(latestChanges: DataFrame): DeltaMergeBuilder = {
    DeltaTable
      .forPath(destination)
      .as("currentTable")
      .merge(
        latestChanges.as("changes"), s"currentTable.$keyColumn = changes.$keyColumn AND currentTable.$timestampColumn = changes.$timestampColumn"
      )
      .whenMatched()
      .updateAll()
      .whenNotMatched()
      .insertAll()
  }
}

object DeltaCDCToSCD2Writer extends DeltaCDCToSCD2WriterAttributes with StreamWriterFactory {
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


    logger.info(s"Going to create DeltaStreamWriter instance using: " +
      s"destination directory='$destinationDirectory', trigger='$trigger', checkpointLocation='$checkpointLocation', " +
      s"partition columns='$partitionColumns', key column='$keyColumn', timestamp column='$timestampColumn', operation column='$operationColumn', " +
      s"operation delete values='$operationDeleteValues', precombine columns='$precombineColumns', " +
      s"precombine columns custom order='$precombineColumnsCustomOrder', extra options='$extraOptions'")

    new DeltaCDCToSCD2Writer(destinationDirectory, trigger, checkpointLocation, partitionColumns, keyColumn, timestampColumn, operationColumn, operationDeleteValues, precombineColumns, precombineColumnsCustomOrder, extraOptions)
  }

  def getDestinationDirectory(configuration: Configuration): String = ConfigUtils.getOrThrow(KEY_DESTINATION_DIRECTORY, configuration, errorMessage = s"Destination directory not found. Is '$KEY_DESTINATION_DIRECTORY' defined?")

  def getExtraOptions(configuration: Configuration): Map[String, String] = ConfigUtils.getPropertySubset(configuration, KEY_EXTRA_CONFS_ROOT)

  override def getExtraConfigurationPrefix: Option[String] = Some(KEY_EXTRA_CONFS_ROOT)
}
