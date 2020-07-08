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

import org.apache.commons.configuration2.BaseConfiguration
import org.scalatest.{FlatSpec, Matchers}
import za.co.absa.abris.avro.read.confluent.SchemaManager
import za.co.absa.abris.avro.read.confluent.SchemaManager.PARAM_SCHEMA_REGISTRY_URL
import za.co.absa.hyperdrive.ingestor.implementation.transformer.avro.confluent.ConfluentAvroEncodingTransformer._
import za.co.absa.hyperdrive.ingestor.implementation.writer.kafka.KafkaStreamWriter

class TestConfluentAvroEncodingTransformer extends FlatSpec with Matchers {

  private val topic = "topic"
  private val schemaRegistryURL = "http://localhost:8081"

  behavior of ConfluentAvroEncodingTransformer.getClass.getSimpleName

  it should "create avro stream encoder instance with schema registry settings for value schema" in {
    val config = new BaseConfiguration
    config.addProperty(KafkaStreamWriter.KEY_TOPIC, topic)
    config.addProperty(KEY_SCHEMA_REGISTRY_URL, schemaRegistryURL)
    config.addProperty(KEY_SCHEMA_REGISTRY_VALUE_NAMING_STRATEGY, SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME)

    val encoder = ConfluentAvroEncodingTransformer(config).asInstanceOf[ConfluentAvroEncodingTransformer]

    encoder.valueSchemaRegistrySettings(PARAM_SCHEMA_REGISTRY_URL) shouldBe schemaRegistryURL
    encoder.keySchemaRegistrySettings shouldBe None
  }

  it should "create avro stream encoder instance with schema registry settings for value schema and key schema" in {
    val config = new BaseConfiguration
    config.addProperty(KafkaStreamWriter.KEY_TOPIC, topic)
    config.addProperty(KEY_PRODUCE_KEYS, "TRUE")
    config.addProperty(KEY_SCHEMA_REGISTRY_URL, schemaRegistryURL)
    config.addProperty(KEY_SCHEMA_REGISTRY_VALUE_NAMING_STRATEGY, SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME)
    config.addProperty(KEY_SCHEMA_REGISTRY_KEY_NAMING_STRATEGY, SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME)

    val encoder = ConfluentAvroEncodingTransformer(config).asInstanceOf[ConfluentAvroEncodingTransformer]

    encoder.valueSchemaRegistrySettings(PARAM_SCHEMA_REGISTRY_URL) shouldBe schemaRegistryURL
    encoder.keySchemaRegistrySettings.get(PARAM_SCHEMA_REGISTRY_URL) shouldBe schemaRegistryURL
  }
}
