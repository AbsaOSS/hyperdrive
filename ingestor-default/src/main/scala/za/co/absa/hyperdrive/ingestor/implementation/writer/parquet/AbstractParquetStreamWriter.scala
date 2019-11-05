/*
 * Copyright 2018-2019 ABSA Group Limited
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
import za.co.absa.hyperdrive.ingestor.api.manager.OffsetManager
import za.co.absa.hyperdrive.ingestor.api.writer.StreamWriter
import za.co.absa.hyperdrive.shared.configurations.ConfigurationsKeys.ParquetStreamWriterKeys.{KEY_DESTINATION_DIRECTORY, KEY_EXTRA_CONFS_ROOT}
import za.co.absa.hyperdrive.shared.utils.ConfigUtils.getOrThrow

import scala.util.{Failure, Success, Try}

private[writer] abstract class AbstractParquetStreamWriter(destination: String, val extraConfOptions: Option[Map[String, String]]) extends StreamWriter(destination) {

  if (StringUtils.isBlank(destination)) {
    throw new IllegalArgumentException(s"Invalid PARQUET destination: '$destination'")
  }

  def write(dataFrame: DataFrame, offsetManager: OffsetManager): StreamingQuery = {
    if (dataFrame == null) {
      throw new IllegalArgumentException("Null DataFrame.")
    }

    if (offsetManager == null) {
      throw new IllegalArgumentException("Null OffsetManager instance.")
    }

    val outStream = getOutStream(dataFrame)

    val streamWithOptions = addOptions(outStream, extraConfOptions)

    val streamWithOptionsAndOffset = configureOffsets(streamWithOptions, offsetManager, dataFrame.sparkSession.sparkContext.hadoopConfiguration)

    streamWithOptionsAndOffset.start(destination)
  }

  protected def getOutStream(dataFrame: DataFrame): DataStreamWriter[Row] = {
    dataFrame
      .writeStream
      .trigger(Trigger.Once())
      .format(source = "parquet")
      .outputMode(OutputMode.Append())
  }

  protected def addOptions(outStream: DataStreamWriter[Row], extraConfOptions: Option[Map[String, String]]): DataStreamWriter[Row] = {
    extraConfOptions match {
      case Some(options) => options.foldLeft(outStream) {
        case (previousOutStream, (optionKey, optionValue)) => previousOutStream.option(optionKey, optionValue)
      }
      case None => outStream
    }
  }

  protected def configureOffsets(outStream: DataStreamWriter[Row], offsetManager: OffsetManager, configuration: org.apache.hadoop.conf.Configuration): DataStreamWriter[Row] = offsetManager.configureOffsets(outStream, configuration)
}

object AbstractParquetStreamWriter {

  def getDestinationDirectory(configuration: Configuration): String = getOrThrow(KEY_DESTINATION_DIRECTORY, configuration, errorMessage = s"Destination directory not found. Is '$KEY_DESTINATION_DIRECTORY' defined?")

  def getExtraOptions(configuration: Configuration): Option[Map[String, String]] = {
    import scala.collection.JavaConverters._
    val extraOptions = configuration.getKeys(KEY_EXTRA_CONFS_ROOT)
      .asScala
      .map(key => getKeyValueConf(key, configuration))

    if (extraOptions.nonEmpty) {
      Some(extraOptions.toMap)
    } else {
      None
    }
  }

  private def getKeyValueConf(key: String, configuration: Configuration): (String, String) = {
    Try(parseConf(configuration.getString(key.toString))) match {
      case Success(keyValue) => keyValue
      case Failure(exception) => throw new IllegalArgumentException(s"Invalid extra configuration for stream writer: '$key'", exception)
    }
  }

  private def parseConf(option: String): (String, String) = {
    val keyValue = option.split("=")
    if (keyValue.length == 2) {
      (keyValue.head.trim(), keyValue.tail.head.trim())
    } else {
      throw new IllegalArgumentException(s"Invalid option: '$option'")
    }
  }
}


