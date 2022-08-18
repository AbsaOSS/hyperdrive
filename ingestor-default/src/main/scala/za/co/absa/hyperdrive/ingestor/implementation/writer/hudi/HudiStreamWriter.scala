
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
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.slf4j.LoggerFactory
import za.co.absa.hyperdrive.compatibility.provider.CompatibleHudiIngestorProvider
import za.co.absa.hyperdrive.ingestor.api.utils.{ConfigUtils, StreamWriterUtil}
import za.co.absa.hyperdrive.ingestor.api.writer.{StreamWriter, StreamWriterFactory, StreamWriterProperties}

private[writer] class HudiStreamWriter(
  destination: String, trigger: Trigger,
  checkpointLocation: String,
  keyColumn: String,
  timestampColumn: String,
  partitionColumns: Seq[String],
  deletedExprOpt: Option[Column],
  val extraConfOptions: Map[String, String]) extends StreamWriter {
  override def write(dataFrame: DataFrame): StreamingQuery = {
    dataFrame.sparkSession.sparkContext.getConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // doesn't work
    val hudiWriteConfigs = CompatibleHudiIngestorProvider.getWriteConfigs(partitionColumns, destination, keyColumn, timestampColumn)
    val dfWithDeletedCol = deletedExprOpt match {
      case Some(deletedExpr) => dataFrame.withColumn("_hoodie_is_deleted", deletedExpr)
      case None => dataFrame
    }
    dfWithDeletedCol.writeStream
      .trigger(trigger)
      .format(source = "hudi")
      .outputMode(OutputMode.Append())
      .option(StreamWriterProperties.CheckpointLocation, checkpointLocation)
      .options(hudiWriteConfigs)
      .options(extraConfOptions)
      .start(destination)
  }
}

object HudiStreamWriter extends StreamWriterFactory with HudiStreamWriterAttributes {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def apply(config: Configuration): StreamWriter = {
    val destinationDirectory = getDestinationDirectory(config)
    val trigger = StreamWriterUtil.getTrigger(config)
    val checkpointLocation = StreamWriterUtil.getCheckpointLocation(config)
    val partitionColumns = ConfigUtils.getSeqOrNone(KEY_PARTITION_COLUMNS, config).getOrElse(Seq())
    val keyColumn = ConfigUtils.getOrThrow(KEY_KEY_COLUMN, config)
    val timestampColumn = ConfigUtils.getOrThrow(KEY_TIMESTAMP_COLUMN, config)
    val opColumnOpt = Option(config.getString(KEY_OP_COLUMN))
    val deletedValueOpt = Option(config.getString(KEY_OP_DELETED_VALUE))
    val deletedExprOpt = (opColumnOpt, deletedValueOpt) match {
      case (None, _) =>
        logger.warn("Deleted value set without op column. Ignoring deleted value")
        None
      case (_, None) =>
        logger.warn("Op column set without deleted value. Ignoring deleted value")
        None
      case (Some(opColumn), Some(deletedValue)) => Some(col(opColumn) === lit(deletedValue))
      case _ => None
    }
    val extraOptions = getExtraOptions(config)

    LoggerFactory.getLogger(this.getClass).info(s"Going to create HudiStreamWriter instance using: " +
      s"destination directory='$destinationDirectory', trigger='$trigger', checkpointLocation='$checkpointLocation', extra options='$extraOptions'")

    new HudiStreamWriter(destinationDirectory, trigger, checkpointLocation, keyColumn, timestampColumn, partitionColumns, deletedExprOpt, extraOptions)
  }
  def getDestinationDirectory(configuration: Configuration): String = ConfigUtils.getOrThrow(KEY_DESTINATION_DIRECTORY, configuration, errorMessage = s"Destination directory not found. Is '$KEY_DESTINATION_DIRECTORY' defined?")

  def getExtraOptions(configuration: Configuration): Map[String, String] = ConfigUtils.getPropertySubset(configuration, KEY_EXTRA_CONFS_ROOT)

  override def getExtraConfigurationPrefix: Option[String] = Some(KEY_EXTRA_CONFS_ROOT)
}
