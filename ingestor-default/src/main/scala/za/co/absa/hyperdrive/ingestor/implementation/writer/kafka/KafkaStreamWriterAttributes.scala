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

package za.co.absa.hyperdrive.ingestor.implementation.writer.kafka

import za.co.absa.hyperdrive.ingestor.api.{HasComponentAttributes, PropertyMetadata}

trait KafkaStreamWriterAttributes extends HasComponentAttributes {

  val keysPrefix = "writer.kafka"
  val optionalConfKey = s"$keysPrefix.option"
  val KEY_TOPIC = s"$keysPrefix.topic"
  val KEY_BROKERS = s"$keysPrefix.brokers"
  val KEY_SCHEMA_REGISTRY_URL = s"$keysPrefix.schema.registry.url"
  val KEY_SCHEMA_REGISTRY_VALUE_NAMING_STRATEGY = s"$keysPrefix.value.schema.naming.strategy"
  val KEY_SCHEMA_REGISTRY_VALUE_RECORD_NAME = s"$keysPrefix.value.schema.record.name"
  val KEY_SCHEMA_REGISTRY_VALUE_RECORD_NAMESPACE = s"$keysPrefix.value.schema.record.namespace"

  /**
   * @return a human readable name of the component.
   */
  override def getName: String = "Kafka Stream Writer"

  /**
   * @return a description for the component.
   */
  override def getDescription: String = "This component writes ingested data to a kafka topic."

  /**
   * @return a map describing configuration properties for this component. The keys have to be unique to avoid
   *         name clashes with properties from other components.
   */
  override def getProperties: Map[String, PropertyMetadata] = Map(
    KEY_TOPIC -> PropertyMetadata("Topic name", None, required = true),
    KEY_BROKERS -> PropertyMetadata("Brokers", Some("Comma-separated list of kafka broker urls"), required = true),
    KEY_SCHEMA_REGISTRY_URL -> PropertyMetadata("Schema Registry URL", None, required = true),
    KEY_SCHEMA_REGISTRY_VALUE_NAMING_STRATEGY -> PropertyMetadata("Schema naming strategy",
      Some("Subject name strategy of Schema Registry. Must be one of \"topic.name\", \"record.name\" or \"topic.record.name\""),
      required = true),
    KEY_SCHEMA_REGISTRY_VALUE_RECORD_NAME -> PropertyMetadata("Record name",
      Some("Record name for naming strategies record.name or topic.record.name"), required = false),
    KEY_SCHEMA_REGISTRY_VALUE_RECORD_NAMESPACE -> PropertyMetadata("Record namespace",
      Some("Record namespace for naming strategies record.name or topic.record.name"), required = false)
  )
}
