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

package za.co.absa.hyperdrive.offset.impl

import org.apache.hadoop.conf.Configuration
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter}
import za.co.absa.hyperdrive.offset.OffsetManager
import za.co.absa.hyperdrive.settings.InfrastructureSettings.{KafkaSettings, SparkSettings}
import za.co.absa.hyperdrive.utils.FileUtils

class CheckpointingOffsetManager(topic: String, configuration: Configuration) extends OffsetManager(topic, configuration) {

  private val logger = LogManager.getLogger

  val checkpointLocation = resolveCheckpointLocation(topic)

  override def configureOffsets(streamReader: DataStreamReader): DataStreamReader = {
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
    logger.info(s"Checkpoint location resolved to: '$checkpointLocation'")
    streamWriter.option(SparkSettings.CHECKPOINT_LOCATION_KEY, checkpointLocation)
  }

  private def resolveCheckpointLocation(topic: String): String = {
    s"${SparkSettings.CHECKPOINT_BASE_LOCATION}/$topic"
  }

  private def getStartingOffsets(checkpointLocation: String, configuration: Configuration): Option[String] = {
    if (FileUtils.exists(checkpointLocation, configuration)) {
      Option.empty
    }
    else {
      Option(KafkaSettings.STARTING_OFFSETS_EARLIEST)
    }
  }
}
