
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

package za.co.absa.hyperdrive.ingestor.implementation.writer.hudi

import org.apache.commons.configuration2.Configuration
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.slf4j.LoggerFactory
import za.co.absa.hyperdrive.ingestor.api.utils.{ConfigUtils, StreamWriterUtil}
import za.co.absa.hyperdrive.ingestor.api.writer.{StreamWriter, StreamWriterFactory}
import za.co.absa.hyperdrive.ingestor.implementation.writer.parquet.ParquetStreamWriterAttributes

private[writer] class HudiStreamWriter(
  destination: String, trigger: Trigger,
  checkpointLocation: String,
  partitionColumns: Option[Seq[String]],
  val extraConfOptions: Map[String, String]) extends StreamWriter {
  override def write(dataFrame: DataFrame): StreamingQuery = {
    val dsw1 = df.writeStream
      .format("hudi")
      .options(getQuickstartWriteConfigs)
      .option(PRECOMBINE_FIELD.key(), timestampColumn)
      .option(RECORDKEY_FIELD.key(), keyColumn)
      .option(KEYGENERATOR_CLASS_NAME.key(), classOf[ComplexKeyGenerator].getName)
      .option(PARTITIONPATH_FIELD.key(), partitionColumns.mkString(","))
      .option(HIVE_STYLE_PARTITIONING.key(), "true")
      .option(TABLE_NAME.key(), destination)
      .option("hoodie.table.name", destination)
      .outputMode(OutputMode.Append())
      .option("checkpointLocation", s"${destination}/_checkpoints")
  }
}

object HudiStreamWriter extends StreamWriterFactory with ParquetStreamWriterAttributes {

  def apply(config: Configuration): StreamWriter = {
    val destinationDirectory = getDestinationDirectory(config)
    val trigger = StreamWriterUtil.getTrigger(config)
    val checkpointLocation = StreamWriterUtil.getCheckpointLocation(config)
    val partitionColumns = ConfigUtils.getSeqOrNone(KEY_PARTITION_COLUMNS, config)
    val extraOptions = getExtraOptions(config)

    LoggerFactory.getLogger(this.getClass).info(s"Going to create ParquetStreamWriter instance using: " +
      s"destination directory='$destinationDirectory', trigger='$trigger', checkpointLocation='$checkpointLocation', extra options='$extraOptions'")

    new HudiStreamWriter(destinationDirectory, trigger, checkpointLocation, partitionColumns, extraOptions)
  }
  def getDestinationDirectory(configuration: Configuration): String = ConfigUtils.getOrThrow(KEY_DESTINATION_DIRECTORY, configuration, errorMessage = s"Destination directory not found. Is '$KEY_DESTINATION_DIRECTORY' defined?")

  def getMetadataCheck(configuration: Configuration): Boolean = ConfigUtils.getOptionalBoolean(KEY_METADATA_CHECK, configuration).getOrElse(false)

  def getExtraOptions(configuration: Configuration): Map[String, String] = ConfigUtils.getPropertySubset(configuration, KEY_EXTRA_CONFS_ROOT)

  override def getExtraConfigurationPrefix: Option[String] = Some(KEY_EXTRA_CONFS_ROOT)
}
