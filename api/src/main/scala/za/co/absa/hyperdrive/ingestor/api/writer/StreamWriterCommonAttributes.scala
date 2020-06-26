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

package za.co.absa.hyperdrive.ingestor.api.writer

import za.co.absa.hyperdrive.ingestor.api.PropertyMetadata

object StreamWriterCommonAttributes {
  private val checkpointDescription = "Path to the checkpoint location. The checkpoint location has to be unique for each workflow"
  private val keysPrefix = "writer.common"
  val keyTriggerType = s"$keysPrefix.trigger.type"
  val keyTriggerProcessingTime = s"$keysPrefix.trigger.processing.time"
  val keyCheckpointBaseLocation = s"$keysPrefix.checkpoint.location"
  val triggerTypeMetadata: PropertyMetadata = PropertyMetadata("Trigger type",
    Some("Trigger type. Either 'Once' or 'ProcessingTime'. 'Once' by default"), required = false)
  val triggerProcessingTimeMetadata: PropertyMetadata = PropertyMetadata("Micro-batch interval (ms)",
    Some("Micro-batch interval in milliseconds. To be used for long-running spark jobs. Leave empty for one-time spark jobs"), required = false)
  val checkpointBaseLocation: PropertyMetadata = PropertyMetadata("Checkpoint Location", Some(checkpointDescription), required = true)
}

object StreamWriterProperties {
  val CheckpointLocation = "checkpointLocation"
}
