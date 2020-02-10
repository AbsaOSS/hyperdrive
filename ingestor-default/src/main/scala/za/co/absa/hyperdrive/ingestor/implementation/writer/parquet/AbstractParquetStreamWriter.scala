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
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Row}
import za.co.absa.hyperdrive.ingestor.api.manager.StreamManager
import za.co.absa.hyperdrive.ingestor.api.writer.StreamWriter
import za.co.absa.hyperdrive.shared.configurations.ConfigurationsKeys.ParquetStreamWriterKeys.{KEY_DESTINATION_DIRECTORY, KEY_EXTRA_CONFS_ROOT}
import za.co.absa.hyperdrive.shared.utils.ConfigUtils
import za.co.absa.hyperdrive.shared.utils.ConfigUtils.getOrThrow

private[writer] abstract class AbstractParquetStreamWriter(destination: String, val extraConfOptions: Map[String, String]) extends StreamWriter(destination) {

  if (StringUtils.isBlank(destination)) {
    throw new IllegalArgumentException(s"Invalid PARQUET destination: '$destination'")
  }

  def write(dataFrame: DataFrame, streamManager: StreamManager): StreamingQuery = {
    if (dataFrame == null) {
      throw new IllegalArgumentException("Null DataFrame.")
    }

    if (streamManager == null) {
      throw new IllegalArgumentException("Null StreamManager instance.")
    }

    val outStream = getOutStream(dataFrame)

    val streamWithOptions = addOptions(outStream, extraConfOptions)

    val streamWithOptionsAndOffset = configureOffsets(streamWithOptions, streamManager, dataFrame.sparkSession.sparkContext.hadoopConfiguration)

    streamWithOptionsAndOffset.start(destination)
  }

  protected def getOutStream(dataFrame: DataFrame): DataStreamWriter[Row] = {
    dataFrame
      .writeStream
      .trigger(Trigger.Once())
      .format(source = "parquet")
      .outputMode(OutputMode.Append())
  }

  protected def addOptions(outStream: DataStreamWriter[Row], extraConfOptions: Map[String, String]): DataStreamWriter[Row] = outStream.options(extraConfOptions)

  protected def configureOffsets(outStream: DataStreamWriter[Row], streamManager: StreamManager, configuration: org.apache.hadoop.conf.Configuration): DataStreamWriter[Row] = streamManager.configure(outStream, configuration)
}

object AbstractParquetStreamWriter {

  def getDestinationDirectory(configuration: Configuration): String = getOrThrow(KEY_DESTINATION_DIRECTORY, configuration, errorMessage = s"Destination directory not found. Is '$KEY_DESTINATION_DIRECTORY' defined?")

  def getExtraOptions(configuration: Configuration): Map[String, String] = ConfigUtils.getPropertySubset(configuration, KEY_EXTRA_CONFS_ROOT)
}


