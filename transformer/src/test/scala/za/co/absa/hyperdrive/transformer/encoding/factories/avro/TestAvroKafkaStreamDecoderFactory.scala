/*
 *  Copyright 2019 ABSA Group Limited
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package za.co.absa.hyperdrive.transformer.encoding.factories.avro

import org.apache.commons.configuration2.Configuration
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FlatSpec}
import org.mockito.Mockito._
import za.co.absa.abris.avro.read.confluent.SchemaManager
import za.co.absa.hyperdrive.shared.configurations.ConfigurationsKeys.AvroKafkaStreamDecoderKeys._
import za.co.absa.hyperdrive.transformer.encoding.impl.avro.AvroKafkaStreamDecoder

class TestAvroKafkaStreamDecoderFactory extends FlatSpec with BeforeAndAfterEach with MockitoSugar {

  private val topic = "topic"
  private val schemaRetentionPolicy = "RETAIN_SELECTED_COLUMN_ONLY"
  private val schemaRegistryURL = "http://localhost:8081"
  private val schemaRegistryValueNamingStrategy = "topic.name"
  private val schemaRegistryValueSchemaId = "latest"

  private val schemaRegistrySettings = Map[String,String](
    KEY_SCHEMA_REGISTRY_URL -> schemaRegistryURL,
    KEY_SCHEMA_REGISTRY_VALUE_NAMING_STRATEGY -> schemaRegistryValueNamingStrategy,
    KEY_SCHEMA_REGISTRY_VALUE_SCHEMA_ID -> schemaRegistryValueSchemaId
  )

  private val configStub = mock[Configuration]

  override def beforeEach(): Unit = reset(configStub)

  behavior of AvroKafkaStreamDecoderFactory.getClass.getSimpleName

  it should "throw if topic is not informed" in {
    stubSchemaRegistrySettings()
    stubSchemaRetentionPolicy()

    val throwable = intercept[IllegalArgumentException](AvroKafkaStreamDecoderFactory.build(configStub))
    assert(throwable.getMessage.toLowerCase.contains("topic"))
  }

  it should "throw if retention policy is missing" in {
    stubTopic()
    stubSchemaRegistrySettings()

    val throwable = intercept[IllegalArgumentException](AvroKafkaStreamDecoderFactory.build(configStub))
    assert(throwable.getMessage.toLowerCase.contains("retention"))
  }

  it should "throw if invalid retention policy specified" in {
    stubTopic()
    stubSchemaRegistrySettings()
    stubSchemaRetentionPolicy(policy = "invalid-retention-policy")

    val throwable = intercept[IllegalArgumentException](AvroKafkaStreamDecoderFactory.build(configStub))
    assert(throwable.getMessage.toLowerCase.contains("retention"))
  }

  it should "throw if schema registry configurations are missing" in {
    Map[String,String](
      KEY_SCHEMA_REGISTRY_URL -> "url",
      KEY_SCHEMA_REGISTRY_VALUE_SCHEMA_ID -> "id",
      KEY_SCHEMA_REGISTRY_VALUE_NAMING_STRATEGY -> "strategy"
    )
        .foreach {
          case(configKey,expectedTokenInError) =>
            beforeEach()
            stubTopic()
            stubSchemaRetentionPolicy()
            stubSchemaRegistrySettings(Set(configKey))

            val throwable = intercept[IllegalArgumentException](AvroKafkaStreamDecoderFactory.build(configStub))
            assert(throwable.getMessage.toLowerCase.contains(expectedTokenInError))
        }
  }

  it should "create avro stream decoder instance" in {
    stubTopic()
    stubSchemaRetentionPolicy()
    stubSchemaRegistrySettings()

    val decoder = AvroKafkaStreamDecoderFactory.build(configStub).asInstanceOf[AvroKafkaStreamDecoder]

    assert(topic == decoder.topic)
    assert(schemaRetentionPolicy == decoder.retentionPolicy.toString)
    assert(schemaRegistrySettings(KEY_SCHEMA_REGISTRY_URL) == decoder.schemaRegistrySettings(SchemaManager.PARAM_SCHEMA_REGISTRY_URL))
    assert(schemaRegistrySettings(KEY_SCHEMA_REGISTRY_VALUE_SCHEMA_ID) == decoder.schemaRegistrySettings(SchemaManager.PARAM_VALUE_SCHEMA_ID))
    assert(schemaRegistrySettings(KEY_SCHEMA_REGISTRY_VALUE_NAMING_STRATEGY) == decoder.schemaRegistrySettings(SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY))
  }

  private def stubProperty(key: String, value: String): Unit = when(configStub.getString(key)).thenReturn(value)

  private def stubTopic(): Unit = stubProperty(KEY_TOPIC, topic)

  private def stubSchemaRetentionPolicy(policy: String = schemaRetentionPolicy): Unit = stubProperty(KEY_SCHEMA_RETENTION_POLICY, policy)

  private def stubSchemaRegistrySettings(keysToExclude: Set[String] = Set[String]()): Unit = {
      schemaRegistrySettings
      .filterKeys(!keysToExclude.contains(_))
      .foreach {case(key,value) => stubProperty(key, value)}
  }
}
