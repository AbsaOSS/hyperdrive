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

package za.co.absa.hyperdrive.ingestor.implementation.transformer.avro.confluent

import za.co.absa.hyperdrive.ingestor.api.{HasComponentAttributes, PropertyMetadata}

trait ConfluentAvroEncodingTransformerAttributes extends HasComponentAttributes {
  val KEY_SCHEMA_REGISTRY_URL = "schema.registry.url"
  val KEY_SCHEMA_REGISTRY_VALUE_NAMING_STRATEGY = "value.schema.naming.strategy"
  val KEY_SCHEMA_REGISTRY_VALUE_RECORD_NAME = "value.schema.record.name"
  val KEY_SCHEMA_REGISTRY_VALUE_RECORD_NAMESPACE = "value.schema.record.namespace"

  val KEY_PRODUCE_KEYS = "produce.keys"
  val KEY_SCHEMA_REGISTRY_KEY_NAMING_STRATEGY = "key.schema.naming.strategy"
  val KEY_SCHEMA_REGISTRY_KEY_RECORD_NAME = "key.schema.record.name"
  val KEY_SCHEMA_REGISTRY_KEY_RECORD_NAMESPACE = "key.schema.record.namespace"

  override def getName: String = "Confluent Avro Stream Encoder"

  override def getDescription: String = "Encoder for records in Avro format. The encoder connects to a Schema Registry instance to update the schema information."

  override def getProperties: Map[String, PropertyMetadata] = Map(
    KEY_SCHEMA_REGISTRY_URL -> PropertyMetadata("Schema Registry URL", None, required = true),
    KEY_SCHEMA_REGISTRY_VALUE_NAMING_STRATEGY -> PropertyMetadata("Value-Schema naming strategy",
      Some("Subject name strategy of Schema Registry. Must be one of \"topic.name\", \"record.name\" or \"topic.record.name\""),
      required = true),
    KEY_SCHEMA_REGISTRY_VALUE_RECORD_NAME -> PropertyMetadata("Value-Record name",
      Some("Record name for naming strategies record.name or topic.record.name"), required = false),
    KEY_SCHEMA_REGISTRY_VALUE_RECORD_NAMESPACE -> PropertyMetadata("Value-Record namespace",
      Some("Record namespace for naming strategies record.name or topic.record.name"), required = false),

    KEY_SCHEMA_REGISTRY_KEY_NAMING_STRATEGY -> PropertyMetadata("Key-Schema naming strategy",
      Some("Subject name strategy of Schema Registry. Must be one of \"topic.name\", \"record.name\" or \"topic.record.name\""), required = false),
    KEY_SCHEMA_REGISTRY_KEY_RECORD_NAME -> PropertyMetadata("Key-Record name", Some("Key-Record name for naming strategies record.name or topic.record.name"), required = false),
    KEY_SCHEMA_REGISTRY_KEY_RECORD_NAMESPACE -> PropertyMetadata("Key-Record namespace", Some("Key-Record namespace for naming strategies record.name or topic.record.name"), required = false)
  )
}
