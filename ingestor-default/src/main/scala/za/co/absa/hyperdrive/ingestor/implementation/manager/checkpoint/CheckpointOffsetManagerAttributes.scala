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

import za.co.absa.hyperdrive.ingestor.api.{HasComponentAttributes, PropertyMetadata}
import za.co.absa.hyperdrive.shared.configurations.ConfigurationsKeys.CheckpointOffsetManagerKeys.KEY_CHECKPOINT_BASE_LOCATION
import za.co.absa.hyperdrive.shared.configurations.ConfigurationsKeys.KafkaStreamReaderKeys.KEY_STARTING_OFFSETS

trait CheckpointOffsetManagerAttributes extends HasComponentAttributes {

  override def getName: String = "Checkpoint Offset Manager"

  override def getDescription: String = "Configures the checkpoint location for both reader and writer."

  override def getProperties: Map[String, PropertyMetadata] = {
    val checkpointDescription = "Path to the checkpoint location. The checkpoint location has to be unique for each workflow"
    val offsetDescription = "The starting offset is only considered if the checkpoint location does not already exist," +
      "i.e. the ingestion has not been started yet."
    Map(
      KEY_CHECKPOINT_BASE_LOCATION -> PropertyMetadata("Checkpoint Location", Some(checkpointDescription), required = true),
      KEY_STARTING_OFFSETS -> PropertyMetadata("Starting offset", Some(offsetDescription), required = false)
    )
  }
}
