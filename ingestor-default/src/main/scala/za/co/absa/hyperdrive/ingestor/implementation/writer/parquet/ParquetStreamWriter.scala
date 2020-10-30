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

package za.co.absa.hyperdrive.ingestor.implementation.writer.parquet

import org.apache.commons.configuration2.Configuration
import org.apache.commons.lang3.StringUtils
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import za.co.absa.hyperdrive.ingestor.api.utils.{ConfigUtils, StreamWriterUtil}
import za.co.absa.hyperdrive.ingestor.api.writer.{StreamWriter, StreamWriterFactory, StreamWriterProperties}
import za.co.absa.hyperdrive.ingestor.implementation.utils.MetadataLogUtil
import za.co.absa.hyperdrive.shared.configurations.ConfigurationsKeys.ParquetStreamWriterKeys
import za.co.absa.hyperdrive.shared.configurations.ConfigurationsKeys.ParquetStreamWriterKeys._

import scala.util.{Failure, Success}

private[writer] class ParquetStreamWriter(destination: String, trigger: Trigger,
                                          checkpointLocation: String,
                                          partitionColumns: Option[Seq[String]],
                                          doMetadataCheck: Boolean,
                                          val extraConfOptions: Map[String, String]) extends StreamWriter {
  private val logger = LogManager.getLogger
  if (StringUtils.isBlank(destination)) {
    throw new IllegalArgumentException(s"Invalid PARQUET destination: '$destination'")
  }

  override def write(dataFrame: DataFrame): StreamingQuery = {

    if (doMetadataCheck) {
      MetadataLogUtil.getParquetFilesNotListedInMetadataLog(dataFrame.sparkSession, destination) match {
        case Failure(exception) => throw exception
        case Success(inconsistentFiles) if inconsistentFiles.nonEmpty => throw new IllegalStateException(
          "Inconsistent Metadata Log. The following files are on the filesystem, but not in the metadata log," +
            "most probably due to a previous partial write. If that is the case, they should be removed.\n " +
            s"${inconsistentFiles.reduce(_ + "\n" + _)}")
        case _ => // do nothing
      }
    }

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
      .options(extraConfOptions)
      .option(StreamWriterProperties.CheckpointLocation, checkpointLocation)
      .start(destination)
  }

  def getDestination: String = destination
}

object ParquetStreamWriter extends StreamWriterFactory with ParquetStreamWriterAttributes {

  def apply(config: Configuration): StreamWriter = {
    val destinationDirectory = getDestinationDirectory(config)
    val doMetadataCheck = getMetadataCheck(config)
    val trigger = StreamWriterUtil.getTrigger(config)
    val checkpointLocation = StreamWriterUtil.getCheckpointLocation(config)
    val partitionColumns = ConfigUtils.getSeqOrNone(ParquetStreamWriterKeys.KEY_PARTITION_COLUMNS, config)
    val extraOptions = getExtraOptions(config)

    LogManager.getLogger.info(s"Going to create ParquetStreamWriter instance using: " +
      s"destination directory='$destinationDirectory', trigger='$trigger', checkpointLocation='$checkpointLocation', extra options='$extraOptions'")

    new ParquetStreamWriter(destinationDirectory, trigger, checkpointLocation, partitionColumns, doMetadataCheck, extraOptions)
  }
  def getDestinationDirectory(configuration: Configuration): String = ConfigUtils.getOrThrow(KEY_DESTINATION_DIRECTORY, configuration, errorMessage = s"Destination directory not found. Is '$KEY_DESTINATION_DIRECTORY' defined?")

  def getMetadataCheck(configuration: Configuration): Boolean = ConfigUtils.getOrNone(KEY_METADATA_CHECK, configuration).isDefined

  def getExtraOptions(configuration: Configuration): Map[String, String] = ConfigUtils.getPropertySubset(configuration, KEY_EXTRA_CONFS_ROOT)

  override def getExtraConfigurationPrefix: Option[String] = Some(KEY_EXTRA_CONFS_ROOT)
}
