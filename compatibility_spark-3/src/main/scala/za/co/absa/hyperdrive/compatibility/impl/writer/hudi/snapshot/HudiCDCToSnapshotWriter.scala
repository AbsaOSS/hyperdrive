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

package za.co.absa.hyperdrive.compatibility.impl.writer.hudi.snapshot

import org.apache.commons.configuration2.Configuration
import org.apache.commons.lang3.StringUtils
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.hudi.keygen.SimpleKeyGenerator
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.slf4j.LoggerFactory
import za.co.absa.hyperdrive.ingestor.api.utils.{ConfigUtils, StreamWriterUtil}
import za.co.absa.hyperdrive.ingestor.api.writer.{StreamWriter, StreamWriterFactory}

private[writer] class HudiCDCToSnapshotWriter(destination: String,
                                               trigger: Trigger,
                                               checkpointLocation: String,
                                               partitionColumns: Seq[String],
                                               keyColumn: String,
                                               operationColumn: String,
                                               operationDeleteValues: Seq[String],
                                               precombineColumn: String,
                                               val extraConfOptions: Map[String, String]) extends StreamWriter {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val CheckpointLocation = "checkpointLocation"

  if (StringUtils.isBlank(destination)) {
    throw new IllegalArgumentException("Destination must not be blank!")
  }

  override def write(dataFrame: DataFrame): StreamingQuery = {
    val dfWithDeletedCol = if (operationDeleteValues.nonEmpty) {
      dataFrame.withColumn("_hoodie_is_deleted", col(operationColumn).isInCollection(operationDeleteValues))
    } else {
      dataFrame
    }

    val dsw1 = dfWithDeletedCol.writeStream
      .format("hudi")
      .options(getQuickstartWriteConfigs)
      .option(PRECOMBINE_FIELD.key(), precombineColumn)
      .option(RECORDKEY_FIELD.key(), keyColumn)
      .option(KEYGENERATOR_CLASS_NAME.key(), classOf[SimpleKeyGenerator].getName)
      .option(PARTITIONPATH_FIELD.key(), partitionColumns.mkString(","))
      .option(HIVE_STYLE_PARTITIONING.key(), "true")
      .option(TABLE_NAME.key(), destination)
      .option("hoodie.table.name", destination)
      .outputMode(OutputMode.Append())
      .option(CheckpointLocation, s"${destination}/_checkpoints")
      .options(extraConfOptions)
    dsw1.start(destination)
  }
}

object HudiCDCToSnapshotWriter extends StreamWriterFactory with HudiCDCToSnapshotWriterAttributes {
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
    val precombineColumn = ConfigUtils.getOrThrow(KEY_PRECOMBINE_COLUMN, config)

    logger.info(s"Going to create DeltaStreamWriter instance using: " +
      s"destination directory='$destinationDirectory', trigger='$trigger', checkpointLocation='$checkpointLocation', " +
      s"partition columns='$partitionColumns', key column='$keyColumn', operation column='$operationColumn', " +
      s"operation delete values='$operationDeleteValues', precombine columns='$precombineColumn', " +
      s"extra options='$extraOptions'")

    new HudiCDCToSnapshotWriter(destinationDirectory, trigger, checkpointLocation, partitionColumns, keyColumn, operationColumn, operationDeleteValues, precombineColumn, extraOptions)
  }

  def getDestinationDirectory(configuration: Configuration): String = ConfigUtils.getOrThrow(KEY_DESTINATION_DIRECTORY, configuration, errorMessage = s"Destination directory not found. Is '$KEY_DESTINATION_DIRECTORY' defined?")

  def getExtraOptions(configuration: Configuration): Map[String, String] = ConfigUtils.getPropertySubset(configuration, KEY_EXTRA_CONFS_ROOT)

  override def getExtraConfigurationPrefix: Option[String] = Some(KEY_EXTRA_CONFS_ROOT)
}
