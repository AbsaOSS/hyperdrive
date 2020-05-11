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
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Row}
import za.co.absa.hyperdrive.ingestor.api.manager.StreamManager
import za.co.absa.hyperdrive.ingestor.api.utils.ConfigUtils.getOrThrow
import za.co.absa.hyperdrive.ingestor.api.utils.{ConfigUtils, StreamWriterUtil}
import za.co.absa.hyperdrive.ingestor.api.writer.StreamWriter
import za.co.absa.hyperdrive.shared.configurations.ConfigurationsKeys.ParquetStreamWriterKeys._

private[writer] abstract class AbstractParquetStreamWriter(destination: String, trigger: Trigger,
                                                           val partitionColumns: Option[Seq[String]],
                                                           val extraConfOptions: Map[String, String]) extends StreamWriter {
  private val logger = LogManager.getLogger
  if (StringUtils.isBlank(destination)) {
    throw new IllegalArgumentException(s"Invalid PARQUET destination: '$destination'")
  }

  override def write(dataFrame: DataFrame, streamManager: StreamManager): StreamingQuery = {
    val outDataframe = transformDataframe(dataFrame)
    val outStream = getOutStream(outDataframe)

    val streamWithOptions = addOptions(outStream, extraConfOptions)

    val streamWithOptionsAndOffset = configureOffsets(streamWithOptions, streamManager, outDataframe.sparkSession.sparkContext.hadoopConfiguration)

    logger.info(s"Writing to $destination")
    streamWithOptionsAndOffset.start(destination)
  }

  def getDestination: String = destination

  /**
   * This method has only been added to preserve existing functionality of ParquetPartitioningStreamWriter.
   * This method will be removed when ParquetPartitioningStreamWriter has been refactored to a Transformer.
   * @see https://github.com/AbsaOSS/hyperdrive/issues/118
   */
  @deprecated
  protected def transformDataframe(dataFrame: DataFrame): DataFrame = dataFrame

  private def getOutStream(dataFrame: DataFrame): DataStreamWriter[Row] = {
    val dataStreamWriter = dataFrame.writeStream
    val dataStreamWriterWithPartition = partitionColumns match {
      case Some(columns) => dataStreamWriter.partitionBy(columns: _*)
      case None => dataStreamWriter
    }
    dataStreamWriterWithPartition
      .trigger(trigger)
      .format(source = "parquet")
      .outputMode(OutputMode.Append())
  }

  private def addOptions(outStream: DataStreamWriter[Row], extraConfOptions: Map[String, String]): DataStreamWriter[Row] = outStream.options(extraConfOptions)

  private def configureOffsets(outStream: DataStreamWriter[Row], streamManager: StreamManager, configuration: org.apache.hadoop.conf.Configuration): DataStreamWriter[Row] = streamManager.configure(outStream, configuration)
}

object AbstractParquetStreamWriter {

  def getDestinationDirectory(configuration: Configuration): String = getOrThrow(KEY_DESTINATION_DIRECTORY, configuration, errorMessage = s"Destination directory not found. Is '$KEY_DESTINATION_DIRECTORY' defined?")

  def getExtraOptions(configuration: Configuration): Map[String, String] = ConfigUtils.getPropertySubset(configuration, KEY_EXTRA_CONFS_ROOT)
}


