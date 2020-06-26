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

import za.co.absa.hyperdrive.ingestor.api.writer.StreamWriterCommonAttributes
import za.co.absa.hyperdrive.ingestor.api.{HasComponentAttributes, PropertyMetadata}
import za.co.absa.hyperdrive.shared.configurations.ConfigurationsKeys.ParquetStreamWriterKeys._

trait ParquetStreamWriterAttributes extends HasComponentAttributes {

  override def getName: String = "Parquet Stream Writer"

  override def getDescription: String = "This writer saves ingested data in Parquet format on a filesystem (e.g. HDFS)"

  override def getProperties: Map[String, PropertyMetadata] = Map(
    KEY_DESTINATION_DIRECTORY -> PropertyMetadata("Destination directory", Some("A path to a directory"), required = true),
    KEY_PARTITION_COLUMNS -> PropertyMetadata("Partition columns", Some("Comma-separated list of columns to partition by"), required = false),
    KEY_METADATA_CHECK -> PropertyMetadata("Metadata check", Some("Set this option if the consistency of the metadata log should be checked prior to the query. " +
      "For very large tables, the check may be very expensive"), required = false),
    StreamWriterCommonAttributes.keyTriggerProcessingTime -> StreamWriterCommonAttributes.triggerProcessingTimeMetadata,
    StreamWriterCommonAttributes.keyCheckpointBaseLocation -> StreamWriterCommonAttributes.checkpointBaseLocation
  )
}
