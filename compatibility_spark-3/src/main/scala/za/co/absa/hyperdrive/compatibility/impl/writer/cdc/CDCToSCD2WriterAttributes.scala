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

package za.co.absa.hyperdrive.compatibility.impl.writer.cdc

import za.co.absa.hyperdrive.ingestor.api.writer.StreamWriterCommonAttributes
import za.co.absa.hyperdrive.ingestor.api.{HasComponentAttributes, PropertyMetadata}

trait CDCToSCD2WriterAttributes extends HasComponentAttributes {
  val rootFactoryConfKey: String
  val KEY_DESTINATION_DIRECTORY = s"$rootFactoryConfKey.destination.directory"
  val KEY_EXTRA_CONFS_ROOT = s"$rootFactoryConfKey.options"
  val KEY_PARTITION_COLUMNS = s"$rootFactoryConfKey.partition.columns"
  val KEY_KEY_COLUMN = s"$rootFactoryConfKey.key.column"
  val KEY_TIMESTAMP_COLUMN = s"$rootFactoryConfKey.timestamp.column"
  val KEY_OPERATION_COLUMN = s"$rootFactoryConfKey.operation.column"
  val KEY_OPERATION_DELETED_VALUES = s"$rootFactoryConfKey.operation.deleted.values"
  val KEY_PRECOMBINE_COLUMNS = s"$rootFactoryConfKey.precombineColumns"
  val KEY_PRECOMBINE_COLUMNS_CUSTOM_ORDER = s"$rootFactoryConfKey.precombineColumns.customOrder"

  override def getName: String

  override def getDescription: String

  override def getProperties: Map[String, PropertyMetadata] = Map(
    KEY_DESTINATION_DIRECTORY -> PropertyMetadata("Destination directory", Some("A path to a directory"), required = true),
    KEY_PARTITION_COLUMNS -> PropertyMetadata("Partition columns", Some("Comma-separated list of columns to partition by"), required = false),
    KEY_KEY_COLUMN -> PropertyMetadata("Key column", Some("A column with unique entity identifier"), required = true),
    KEY_TIMESTAMP_COLUMN -> PropertyMetadata("Timestamp column", Some("A column with timestamp"), required = true),
    KEY_OPERATION_COLUMN -> PropertyMetadata("Operation column", Some("A column containing value marking a record with an operation"), required = true),
    KEY_OPERATION_DELETED_VALUES -> PropertyMetadata("Delete values for Operation column", Some("Values marking a record for deletion in the operation column"), required = true),
    KEY_PRECOMBINE_COLUMNS -> PropertyMetadata("Precombine columns", Some("When two records have the same key value, we will pick the one with the largest value for precombine columns. Evaluated in provided order"), required = true),
    KEY_PRECOMBINE_COLUMNS_CUSTOM_ORDER -> PropertyMetadata("Precombine columns' custom order", Some("Precombine column's custom order"), required = false),
    StreamWriterCommonAttributes.keyTriggerProcessingTime -> StreamWriterCommonAttributes.triggerProcessingTimeMetadata,
    StreamWriterCommonAttributes.keyCheckpointBaseLocation -> StreamWriterCommonAttributes.checkpointBaseLocation
  )
}
