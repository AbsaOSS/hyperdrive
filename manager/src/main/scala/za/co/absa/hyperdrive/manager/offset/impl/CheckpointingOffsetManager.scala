/*
 *
 * Copyright 2019 ABSA Group Limited
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package za.co.absa.hyperdrive.manager.offset.impl

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter}
import za.co.absa.hyperdrive.manager.offset.OffsetManager
import za.co.absa.hyperdrive.shared.InfrastructureSettings.{KafkaSettings, SparkSettings}
import za.co.absa.hyperdrive.shared.utils.FileUtils

class CheckpointingOffsetManager(topic: String, checkpointBaseLocation: String, configuration: Configuration) extends OffsetManager(topic, configuration) {

  if (StringUtils.isBlank(topic)) {
    throw new IllegalArgumentException(s"Invalid topic: '$topic'")
  }

  if (configuration == null) {
    throw new IllegalArgumentException("Null Configuration instance.")
  }

  if (isInvalidCheckpointBaseLocation) {
    throw new IllegalArgumentException(s"Invalid base checkpoint location: '$checkpointBaseLocation'. Does it exists and is a directory?")
  }

  private val logger = LogManager.getLogger

  private val checkpointLocation = resolveCheckpointLocation(topic)

  override def configureOffsets(streamReader: DataStreamReader): DataStreamReader = {
    if (streamReader == null) {
      throw new IllegalArgumentException("Null DataStreamReader instance.")
    }

    val startingOffsets = getStartingOffsets(checkpointLocation, configuration)

    if (startingOffsets.isDefined) {
      logger.info(s"Setting starting offsets for topic '$topic' = ${startingOffsets.get}.")
      streamReader.option(KafkaSettings.STARTING_OFFSETS_KEY, startingOffsets.get)
    } else {
      logger.info(s"No offsets to set for topic '$topic'.")
      streamReader
    }
  }

  override def configureOffsets(streamWriter: DataStreamWriter[Row]): DataStreamWriter[Row] = {
    if (streamWriter == null) {
      throw new IllegalArgumentException("Null DataStreamWriter instance.")
    }

    logger.info(s"Checkpoint location resolved to: '$checkpointLocation' for topic '$topic'")
    streamWriter.option(SparkSettings.CHECKPOINT_LOCATION_KEY, checkpointLocation)
  }

  private def resolveCheckpointLocation(topic: String): String = {
    s"$checkpointBaseLocation/$topic"
  }

  private def getStartingOffsets(checkpointLocation: String, configuration: Configuration): Option[String] = {
    if (FileUtils.exists(checkpointLocation, configuration)) {
      Option.empty
    }
    else {
      Option(KafkaSettings.STARTING_OFFSETS_EARLIEST)
    }
  }

  private def isInvalidCheckpointBaseLocation: Boolean = {
    StringUtils.isBlank(checkpointBaseLocation) ||
      FileUtils.notExists(checkpointBaseLocation, configuration) ||
      FileUtils.isNotDirectory(checkpointBaseLocation, configuration)
  }
}
