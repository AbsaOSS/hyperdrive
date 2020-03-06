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

package za.co.absa.hyperdrive.ingestor.implementation.manager.checkpoint

import org.apache.commons.configuration2.Configuration
import org.apache.commons.lang3.StringUtils
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter}
import za.co.absa.hyperdrive.ingestor.api.manager.{StreamManager, StreamManagerFactory}
import za.co.absa.hyperdrive.shared.configurations.ConfigurationsKeys.CheckpointOffsetManagerKeys.KEY_CHECKPOINT_BASE_LOCATION
import za.co.absa.hyperdrive.shared.configurations.ConfigurationsKeys.KafkaStreamReaderKeys.{KEY_STARTING_OFFSETS, WORD_STARTING_OFFSETS}
import za.co.absa.hyperdrive.shared.utils.ConfigUtils.{getOrNone, getOrThrow}
import za.co.absa.hyperdrive.shared.utils.FileUtils

private[manager] object CheckpointOffsetManagerProps {
  val CHECKPOINT_LOCATION_KEY = "checkpointLocation"
  val STARTING_OFFSETS_EARLIEST = "earliest"
}

private[manager] class CheckpointOffsetManager(val checkpointLocation: String,
                                               val startingOffsets: Option[String]) extends StreamManager {

  import CheckpointOffsetManagerProps._

  if (StringUtils.isBlank(checkpointLocation)) {
    throw new IllegalArgumentException(s"Invalid checkpoint location: '$checkpointLocation'")
  }

  private val logger = LogManager.getLogger

  override def configure(streamReader: DataStreamReader, configuration: org.apache.hadoop.conf.Configuration): DataStreamReader = {
    val startingOffsets = getStartingOffsets(checkpointLocation, configuration)

    startingOffsets match {
      case Some(startOffset) =>
        logger.info(s"Setting starting offsets for $startOffset.")
        streamReader.option(WORD_STARTING_OFFSETS, startOffset)
      case _ =>
        logger.info(s"No offsets to set for.")
        streamReader
    }
  }

  override def configure(streamWriter: DataStreamWriter[Row], configuration: org.apache.hadoop.conf.Configuration): DataStreamWriter[Row] = {
    logger.info(s"Checkpoint location resolved to: '$checkpointLocation'")
    streamWriter.option(CHECKPOINT_LOCATION_KEY, checkpointLocation)
  }

  private def getStartingOffsets(checkpointLocation: String, configuration: org.apache.hadoop.conf.Configuration): Option[String] = {
    if (FileUtils.exists(checkpointLocation, configuration)) {
      Option.empty
    }
    else {
      if (startingOffsets.isDefined) startingOffsets else Option(STARTING_OFFSETS_EARLIEST)
    }
  }
}

object CheckpointOffsetManager extends StreamManagerFactory with CheckpointOffsetManagerAttributes {
  override def apply(config: Configuration): StreamManager = {
    val checkpointLocation = getCheckpointLocation(config)
    val startingOffsets = getStartingOffsets(config)

    LogManager.getLogger.info(s"Going to create CheckpointOffsetManager instance using: checkpoint base location='$checkpointLocation'")

    new CheckpointOffsetManager(checkpointLocation, startingOffsets)
  }

  private def getCheckpointLocation(configuration: Configuration): String = getOrThrow(KEY_CHECKPOINT_BASE_LOCATION, configuration, errorMessage = s"Could not find checkpoint location. Is '$KEY_CHECKPOINT_BASE_LOCATION' defined?")

  private def getStartingOffsets(configuration: Configuration): Option[String] = getOrNone(KEY_STARTING_OFFSETS, configuration)
}
