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

import za.co.absa.abris.avro.read.confluent.SchemaManager.{PARAM_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY, PARAM_SCHEMA_NAME_FOR_RECORD_STRATEGY, PARAM_SCHEMA_REGISTRY_URL, PARAM_VALUE_SCHEMA_ID, PARAM_VALUE_SCHEMA_NAMING_STRATEGY}
import za.co.absa.hyperdrive.ingestor.api.{HasComponentAttributes, PropertyMetadata}

trait ConfluentAvroKafkaStreamDecoderAttributes extends HasComponentAttributes {

  override def getName: String = "Confluent Avro Stream Decoder"

  override def getDescription: String = "Decoder for Kafka messages in Avro format. The decoder connects to a Schema Registry instance to retrieve the schema information."

  override def getProperties: Map[String, PropertyMetadata] = Map(
    PARAM_SCHEMA_REGISTRY_URL -> PropertyMetadata("Schema Registry URL", None, required = true),
    PARAM_VALUE_SCHEMA_ID -> PropertyMetadata("Schema Id", Some("Specific Id of schema or \"latest\""), required = true),
    PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> PropertyMetadata("Schema naming strategy",
      Some("Subject name strategy of Schema Registry. Must be one of \"topic.name\", \"record.name\" or \"topic.record.name\""), required = true),
    PARAM_SCHEMA_NAME_FOR_RECORD_STRATEGY -> PropertyMetadata("Record name", Some("Record name for naming strategies record.name or topic.record.name"), required = false),
    PARAM_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY -> PropertyMetadata("Record namespace", Some("Record namespace for naming strategies record.name or topic.record.name"), required = false)
  )

}
