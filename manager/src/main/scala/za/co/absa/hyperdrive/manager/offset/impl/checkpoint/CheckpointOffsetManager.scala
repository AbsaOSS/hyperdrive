/*
 *  Copyright 2019 ABSA Group Limited
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package za.co.absa.hyperdrive.manager.offset.impl.checkpoint

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter}
import za.co.absa.hyperdrive.manager.offset.OffsetManager
import za.co.absa.hyperdrive.shared.utils.FileUtils

private[checkpoint] object CheckpointOffsetManagerProps {
  val STARTING_OFFSETS_KEY      = "startingOffsets"
  val CHECKPOINT_LOCATION_KEY   = "checkpointLocation"
  val STARTING_OFFSETS_EARLIEST = "earliest"
}

private[offset] class CheckpointOffsetManager(val topic: String, val checkpointBaseLocation: String) extends OffsetManager(topic) {

  import CheckpointOffsetManagerProps._

  if (StringUtils.isBlank(topic)) {
    throw new IllegalArgumentException(s"Invalid topic: '$topic'")
  }

  if (StringUtils.isBlank(checkpointBaseLocation)) {
    throw new IllegalArgumentException(s"Invalid checkpoint base location: '$checkpointBaseLocation'")
  }

  private val logger = LogManager.getLogger

  private val checkpointLocation = resolveCheckpointLocation(topic)

  override def configureOffsets(streamReader: DataStreamReader, configuration: Configuration): DataStreamReader = {
    if (streamReader == null) {
      throw new IllegalArgumentException("Null DataStreamReader instance.")
    }

    throwIfInvalidCheckpointLocation(configuration)

    val startingOffsets = getStartingOffsets(checkpointLocation, configuration)

    if (startingOffsets.isDefined) {
      logger.info(s"Setting starting offsets for topic '$topic' = ${startingOffsets.get}.")
      streamReader.option(STARTING_OFFSETS_KEY, startingOffsets.get)
    } else {
      logger.info(s"No offsets to set for topic '$topic'.")
      streamReader
    }
  }

  override def configureOffsets(streamWriter: DataStreamWriter[Row], configuration: Configuration): DataStreamWriter[Row] = {
    if (streamWriter == null) {
      throw new IllegalArgumentException("Null DataStreamWriter instance.")
    }

    throwIfInvalidCheckpointLocation(configuration)

    logger.info(s"Checkpoint location resolved to: '$checkpointLocation' for topic '$topic'")
    streamWriter.option(CHECKPOINT_LOCATION_KEY, checkpointLocation)
  }

  private def resolveCheckpointLocation(topic: String): String = {
    s"$checkpointBaseLocation/$topic"
  }

  private def getStartingOffsets(checkpointLocation: String, configuration: Configuration): Option[String] = {
    if (FileUtils.exists(checkpointLocation, configuration)) {
      Option.empty
    }
    else {
      Option(STARTING_OFFSETS_EARLIEST)
    }
  }

  private def throwIfInvalidCheckpointLocation(configuration: Configuration): Unit = {
    if (StringUtils.isBlank(checkpointBaseLocation) ||
      FileUtils.notExists(checkpointBaseLocation, configuration) ||
      FileUtils.isNotDirectory(checkpointBaseLocation, configuration)) {
      throw new IllegalArgumentException(s"Invalid base checkpoint location: '$checkpointBaseLocation'. Does it exists and is a directory?")
    }
  }
}
