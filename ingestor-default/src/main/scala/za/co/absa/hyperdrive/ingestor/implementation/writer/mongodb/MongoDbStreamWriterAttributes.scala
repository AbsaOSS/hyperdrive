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

package za.co.absa.hyperdrive.ingestor.implementation.writer.mongodb

import za.co.absa.hyperdrive.ingestor.api.writer.StreamWriterCommonAttributes
import za.co.absa.hyperdrive.ingestor.api.{HasComponentAttributes, PropertyMetadata}

trait MongoDbStreamWriterAttributes extends HasComponentAttributes {
  private val rootFactoryConfKey = "writer.mongodb"
  val KEY_URI = s"$rootFactoryConfKey.uri"
  val KEY_DATABASE = s"$rootFactoryConfKey.database"
  val KEY_COLLECTION = s"$rootFactoryConfKey.collection"
  val KEY_EXTRA_CONFS_ROOT = s"$rootFactoryConfKey.options"

  override def getName: String = "Parquet Stream Writer"

  override def getDescription: String = "This writer saves ingested data in Parquet format on a filesystem (e.g. HDFS)"

  override def getProperties: Map[String, PropertyMetadata] = Map(
    KEY_URI -> PropertyMetadata("Output MongoDB URI (should start with 'mongodb://')", Some("Should start with 'mongodb://', e.g. 'mongodb://127.0.0.1/'"), required = true),
    KEY_DATABASE -> PropertyMetadata("Database name", Some("Can be omitted if specified in the URL"), required = false),
    KEY_COLLECTION -> PropertyMetadata("Collection name", Some("Can be omitted if specified in the URL"), required = false),
    StreamWriterCommonAttributes.keyTriggerProcessingTime -> StreamWriterCommonAttributes.triggerProcessingTimeMetadata,
    StreamWriterCommonAttributes.keyCheckpointBaseLocation -> StreamWriterCommonAttributes.checkpointBaseLocation
  )
}
