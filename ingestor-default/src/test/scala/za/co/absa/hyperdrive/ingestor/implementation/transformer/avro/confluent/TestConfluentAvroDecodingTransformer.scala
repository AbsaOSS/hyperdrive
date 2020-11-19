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

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import org.apache.commons.configuration2.BaseConfiguration
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.avro.read.confluent.SchemaManagerFactory
import za.co.absa.abris.config.AbrisConfig
import za.co.absa.hyperdrive.ingestor.implementation.reader.kafka.KafkaStreamReader.KEY_TOPIC
import za.co.absa.hyperdrive.ingestor.implementation.transformer.avro.confluent.ConfluentAvroDecodingTransformer._
import za.co.absa.hyperdrive.ingestor.implementation.utils.AbrisConfigUtil

class TestConfluentAvroDecodingTransformer extends FlatSpec with Matchers with BeforeAndAfter {

  private val Topic = "topic"
  private val SchemaRegistryURL = "dummyUrl"
  private val SchemaRegistryValueSchemaId = "latest"

  private var MockSchemaRegistryClient: MockSchemaRegistryClient = _
  private val DummySchema = AvroSchemaUtils.parse("""{
     "type": "record",
     "name": "default_name",
     "namespace": "default_namespace",
     "fields":[
         {"name": "int", "type":  ["int", "null"] }
     ]
    }""")

  behavior of ConfluentAvroDecodingTransformer.getClass.getSimpleName

  before {
    MockSchemaRegistryClient = new MockSchemaRegistryClient()
    SchemaManagerFactory.resetSRClientInstance()
    SchemaManagerFactory.addSRClientInstance(Map(AbrisConfig.SCHEMA_REGISTRY_URL -> SchemaRegistryURL), MockSchemaRegistryClient)
  }

  "apply" should "throw if topic is not configured" in {
    val config = new BaseConfiguration
    config.addProperty(KEY_SCHEMA_REGISTRY_VALUE_SCHEMA_ID, SchemaRegistryValueSchemaId)

    val throwable = intercept[IllegalArgumentException](ConfluentAvroDecodingTransformer(config))
    assert(throwable.getMessage.toLowerCase.contains("topic"))
  }

  it should "create avro stream decoder instance with schema registry settings for value schema" in {
    MockSchemaRegistryClient.register(s"$Topic-value", DummySchema)
    val config = new BaseConfiguration
    config.addProperty(KEY_TOPIC, Topic)
    config.addProperty(KEY_SCHEMA_REGISTRY_URL, SchemaRegistryURL)
    config.addProperty(KEY_SCHEMA_REGISTRY_VALUE_SCHEMA_ID, SchemaRegistryValueSchemaId)
    config.addProperty(KEY_SCHEMA_REGISTRY_VALUE_NAMING_STRATEGY, AbrisConfigUtil.TopicNameStrategy)

    val decoder = ConfluentAvroDecodingTransformer(config).asInstanceOf[ConfluentAvroDecodingTransformer]

    decoder.valueAvroConfig.schemaRegistryConf.get(AbrisConfig.SCHEMA_REGISTRY_URL) shouldBe SchemaRegistryURL
    decoder.keyAvroConfigOpt shouldBe None
  }

  it should "create avro stream decoder instance with schema registry settings for value schema and key schema" in {
    MockSchemaRegistryClient.register(s"$Topic-key", DummySchema)
    MockSchemaRegistryClient.register(s"$Topic-value", DummySchema)
    val config = new BaseConfiguration
    config.addProperty(KEY_TOPIC, Topic)
    config.addProperty(KEY_CONSUME_KEYS, "TRUE")
    config.addProperty(KEY_SCHEMA_REGISTRY_URL, SchemaRegistryURL)
    config.addProperty(KEY_SCHEMA_REGISTRY_VALUE_SCHEMA_ID, SchemaRegistryValueSchemaId)
    config.addProperty(KEY_SCHEMA_REGISTRY_VALUE_NAMING_STRATEGY, AbrisConfigUtil.TopicNameStrategy)
    config.addProperty(KEY_SCHEMA_REGISTRY_KEY_SCHEMA_ID, SchemaRegistryValueSchemaId)
    config.addProperty(KEY_SCHEMA_REGISTRY_KEY_NAMING_STRATEGY, AbrisConfigUtil.TopicNameStrategy)

    val decoder = ConfluentAvroDecodingTransformer(config).asInstanceOf[ConfluentAvroDecodingTransformer]

    decoder.valueAvroConfig.schemaRegistryConf.get(AbrisConfig.SCHEMA_REGISTRY_URL) shouldBe SchemaRegistryURL
    decoder.keyAvroConfigOpt.get.schemaRegistryConf.get(AbrisConfig.SCHEMA_REGISTRY_URL) shouldBe SchemaRegistryURL
  }

  "determineKeyColumnPrefix" should "return prefix key__ by default" in {
    val columnNames = Seq("abcdef", "foo", "ba")
    val prefix = ConfluentAvroDecodingTransformer.determineKeyColumnPrefix(columnNames)
    prefix shouldBe "key__"
  }

  it should "return a non-conflicting prefix" in {
    val columnNames = Seq("key__abc", "foo", "ba")

    val prefix = ConfluentAvroDecodingTransformer.determineKeyColumnPrefix(columnNames)
    import scala.math.min
    val columnNamesPrefixes = columnNames.map(c => c.substring(0, min(c.length, 5)))
    columnNamesPrefixes should not contain prefix
  }
}
