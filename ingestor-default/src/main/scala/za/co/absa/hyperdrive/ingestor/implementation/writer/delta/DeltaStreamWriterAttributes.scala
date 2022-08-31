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

package za.co.absa.hyperdrive.ingestor.implementation.writer.delta

import za.co.absa.hyperdrive.ingestor.api.writer.StreamWriterCommonAttributes
import za.co.absa.hyperdrive.ingestor.api.{HasComponentAttributes, PropertyMetadata}

trait DeltaStreamWriterAttributes extends HasComponentAttributes {
  private val rootFactoryConfKey = "writer.delta"
  val KEY_DESTINATION_DIRECTORY = s"$rootFactoryConfKey.destination.directory"
  val KEY_EXTRA_CONFS_ROOT = s"$rootFactoryConfKey.options"
  val KEY_PARTITION_COLUMNS = s"$rootFactoryConfKey.partition.columns"
  val KEY_KEY_COLUMN = s"$rootFactoryConfKey.key.column"
  val KEY_TIMESTAMP_COLUMN = s"$rootFactoryConfKey.timestamp.column"
  val KEY_OP_COLUMN = s"$rootFactoryConfKey.op.column"
  val KEY_OP_DELETED_VALUE = s"$rootFactoryConfKey.op.deleted.value"
  val KEY_OP_FILTER_VALUE = s"$rootFactoryConfKey.op.filter.value"

  override def getName: String = "Delta Stream Writer"

  override def getDescription: String = "This writer saves ingested data in Delta format on a filesystem (e.g. HDFS)"

  override def getProperties: Map[String, PropertyMetadata] = Map(
    KEY_DESTINATION_DIRECTORY -> PropertyMetadata("Destination directory", Some("A path to a directory"), required = true),
    KEY_KEY_COLUMN -> PropertyMetadata("Key column", Some("Key column needed for Delta"), required = true),
    KEY_TIMESTAMP_COLUMN -> PropertyMetadata("Timestamp column", Some("Timestamp column needed Delta Hudi"), required = true),
    KEY_OP_COLUMN -> PropertyMetadata("Op column", Some("A column containing values marking a record for deletion"), required = true),
    KEY_OP_DELETED_VALUE -> PropertyMetadata("Delete value for op column", Some("The value marking a record for deletion in the op column"), required = true),
    KEY_OP_FILTER_VALUE -> PropertyMetadata("Filter value for op column", Some("The value marking a record for filter in the op column"), required = true),
    KEY_PARTITION_COLUMNS -> PropertyMetadata("Partition columns", Some("Comma-separated list of columns to partition by"), required = false),
    StreamWriterCommonAttributes.keyTriggerProcessingTime -> StreamWriterCommonAttributes.triggerProcessingTimeMetadata,
    StreamWriterCommonAttributes.keyCheckpointBaseLocation -> StreamWriterCommonAttributes.checkpointBaseLocation
  )
}
