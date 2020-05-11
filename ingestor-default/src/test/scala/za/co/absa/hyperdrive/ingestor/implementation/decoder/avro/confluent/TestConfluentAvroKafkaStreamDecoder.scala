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

import org.apache.commons.configuration2.BaseConfiguration
import org.scalatest.{FlatSpec, Matchers}
import za.co.absa.abris.avro.read.confluent.SchemaManager
import za.co.absa.abris.avro.read.confluent.SchemaManager.PARAM_SCHEMA_REGISTRY_URL
import za.co.absa.hyperdrive.shared.configurations.ConfigurationsKeys.AvroKafkaStreamDecoderKeys._

class TestConfluentAvroKafkaStreamDecoder extends FlatSpec with Matchers {

  private val topic = "topic"
  private val schemaRegistryURL = "http://localhost:8081"
  private val schemaRegistryValueSchemaId = "latest"

  behavior of ConfluentAvroKafkaStreamDecoder.getClass.getSimpleName

  "apply" should "throw if topic is not configured" in {
    val config = new BaseConfiguration

    val throwable = intercept[IllegalArgumentException](ConfluentAvroKafkaStreamDecoder(config))
    assert(throwable.getMessage.toLowerCase.contains("topic"))
  }

  it should "create avro stream decoder instance with schema registry settings for value schema" in {
    val config = new BaseConfiguration
    config.addProperty(KEY_TOPIC, topic)
    config.addProperty(KEY_SCHEMA_REGISTRY_URL, schemaRegistryURL)
    config.addProperty(KEY_SCHEMA_REGISTRY_VALUE_SCHEMA_ID, schemaRegistryValueSchemaId)
    config.addProperty(KEY_SCHEMA_REGISTRY_VALUE_NAMING_STRATEGY, SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME)

    val decoder = ConfluentAvroKafkaStreamDecoder(config).asInstanceOf[ConfluentAvroKafkaStreamDecoder]

    decoder.topic shouldBe topic
    decoder.valueSchemaRegistrySettings(PARAM_SCHEMA_REGISTRY_URL) shouldBe schemaRegistryURL
    decoder.keySchemaRegistrySettings shouldBe None
  }

  it should "create avro stream decoder instance with schema registry settings for value schema and key schema" in {
    val config = new BaseConfiguration
    config.addProperty(KEY_TOPIC, topic)
    config.addProperty(KEY_CONSUME_KEYS, "TRUE")
    config.addProperty(KEY_SCHEMA_REGISTRY_URL, schemaRegistryURL)
    config.addProperty(KEY_SCHEMA_REGISTRY_VALUE_SCHEMA_ID, schemaRegistryValueSchemaId)
    config.addProperty(KEY_SCHEMA_REGISTRY_VALUE_NAMING_STRATEGY, SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME)
    config.addProperty(KEY_SCHEMA_REGISTRY_KEY_SCHEMA_ID, schemaRegistryValueSchemaId)
    config.addProperty(KEY_SCHEMA_REGISTRY_KEY_NAMING_STRATEGY, SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME)

    val decoder = ConfluentAvroKafkaStreamDecoder(config).asInstanceOf[ConfluentAvroKafkaStreamDecoder]

    decoder.topic shouldBe topic
    decoder.valueSchemaRegistrySettings(PARAM_SCHEMA_REGISTRY_URL) shouldBe schemaRegistryURL
    decoder.keySchemaRegistrySettings.get(PARAM_SCHEMA_REGISTRY_URL) shouldBe schemaRegistryURL
  }

  "determineKeyColumnPrefix" should "return prefix key__ by default" in {
    val columnNames = Seq("abcdef", "foo", "ba")
    val prefix = ConfluentAvroKafkaStreamDecoder.determineKeyColumnPrefix(columnNames)
    prefix shouldBe "key__"
  }

  it should "return a non-conflicting prefix" in {
    val columnNames = Seq("key__abc", "foo", "ba")

    val prefix = ConfluentAvroKafkaStreamDecoder.determineKeyColumnPrefix(columnNames)
    import scala.math.min
    val columnNamesPrefixes = columnNames.map(c => c.substring(0, min(c.length, 5)))
    columnNamesPrefixes should not contain prefix
  }
}
