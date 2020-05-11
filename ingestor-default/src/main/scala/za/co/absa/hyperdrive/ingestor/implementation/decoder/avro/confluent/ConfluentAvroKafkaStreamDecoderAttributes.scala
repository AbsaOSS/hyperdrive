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

package za.co.absa.hyperdrive.ingestor.implementation.decoder.avro.confluent

import za.co.absa.hyperdrive.ingestor.api.{HasComponentAttributes, PropertyMetadata}
import za.co.absa.hyperdrive.shared.configurations.ConfigurationsKeys.AvroKafkaStreamDecoderKeys._

trait ConfluentAvroKafkaStreamDecoderAttributes extends HasComponentAttributes {

  override def getName: String = "Confluent Avro Stream Decoder"

  override def getDescription: String = "Decoder for Kafka messages in Avro format. The decoder connects to a Schema Registry instance to retrieve the schema information."

  override def getProperties: Map[String, PropertyMetadata] = Map(
    KEY_SCHEMA_REGISTRY_URL -> PropertyMetadata("Schema Registry URL", None, required = true),

    KEY_SCHEMA_REGISTRY_VALUE_SCHEMA_ID -> PropertyMetadata("Schema Id", Some("Specific Id of value-schema or \"latest\""), required = true),
    KEY_SCHEMA_REGISTRY_VALUE_NAMING_STRATEGY -> PropertyMetadata("Value-Schema naming strategy",
      Some("Subject name strategy of Schema Registry. Must be one of \"topic.name\", \"record.name\" or \"topic.record.name\""), required = true),
    KEY_SCHEMA_REGISTRY_VALUE_RECORD_NAME -> PropertyMetadata("Value-Record name", Some("Value-Record name for naming strategies record.name or topic.record.name"), required = false),
    KEY_SCHEMA_REGISTRY_VALUE_RECORD_NAMESPACE -> PropertyMetadata("Value-Record namespace", Some("Value-Record namespace for naming strategies record.name or topic.record.name"), required = false),

    KEY_CONSUME_KEYS -> PropertyMetadata("Consume keys", Some("True if keys should be ingested as well. Default: False"), required = false),
    KEY_SCHEMA_REGISTRY_KEY_SCHEMA_ID -> PropertyMetadata("Key-Schema Id", Some("Specific Id of key-schema or \"latest\"."), required = false),
    KEY_SCHEMA_REGISTRY_KEY_NAMING_STRATEGY -> PropertyMetadata("Key-Schema naming strategy",
      Some("Subject name strategy of Schema Registry. Must be one of \"topic.name\", \"record.name\" or \"topic.record.name\""), required = false),
    KEY_SCHEMA_REGISTRY_KEY_RECORD_NAME -> PropertyMetadata("Key-Record name", Some("Key-Record name for naming strategies record.name or topic.record.name"), required = false),
    KEY_SCHEMA_REGISTRY_KEY_RECORD_NAMESPACE -> PropertyMetadata("Key-Record namespace", Some("Key-Record namespace for naming strategies record.name or topic.record.name"), required = false)
  )
}
