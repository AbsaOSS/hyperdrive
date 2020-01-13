/*
 * Copyright 2018-2019 ABSA Group Limited
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

import org.apache.commons.configuration2.Configuration
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FlatSpec}
import za.co.absa.abris.avro.read.confluent.SchemaManager
import za.co.absa.abris.avro.read.confluent.SchemaManager._
import za.co.absa.hyperdrive.shared.configurations.ConfigurationsKeys.AvroKafkaStreamDecoderKeys._

class TestConfluentConfluentAvroKafkaStreamDecoderObject extends FlatSpec with BeforeAndAfterEach with MockitoSugar {

  private val topic = "topic"
  private val schemaRegistryURL = "http://localhost:8081"
  private val schemaRegistryValueNamingStrategy = "topic.name"
  private val schemaRegistryValueSchemaId = "latest"

  private val schemaRegistrySettings = Map[String,String](
    KEY_SCHEMA_REGISTRY_URL -> schemaRegistryURL,
    KEY_SCHEMA_REGISTRY_VALUE_NAMING_STRATEGY -> schemaRegistryValueNamingStrategy,
    KEY_SCHEMA_REGISTRY_VALUE_SCHEMA_ID -> schemaRegistryValueSchemaId
  )

  private val configStub = mock[Configuration]

  override def beforeEach(): Unit = org.mockito.Mockito.reset(configStub)

  behavior of ConfluentAvroKafkaStreamDecoder.getClass.getSimpleName

  it should "throw if topic is not informed" in {
    stubSchemaRegistrySettings(schemaRegistrySettings)

    val throwable = intercept[IllegalArgumentException](ConfluentAvroKafkaStreamDecoder(configStub))
    assert(throwable.getMessage.toLowerCase.contains("topic"))
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
            stubSchemaRegistrySettings(schemaRegistrySettings, Set(configKey))

            val throwable = intercept[IllegalArgumentException](ConfluentAvroKafkaStreamDecoder(configStub))
            assert(throwable.getMessage.toLowerCase.contains(expectedTokenInError))
        }
  }

  it should "throw on record name not available if record.name naming strategy" in {
    val settings = schemaRegistrySettings ++ Map[String,String](
      KEY_SCHEMA_REGISTRY_URL -> schemaRegistryURL,
      KEY_SCHEMA_REGISTRY_VALUE_NAMING_STRATEGY -> SchemaStorageNamingStrategies.RECORD_NAME
    )

    stubTopic()
    stubSchemaRegistrySettings(settings)

    val throwable = intercept[IllegalArgumentException](ConfluentAvroKafkaStreamDecoder(configStub))
    assert(throwable.getMessage.toLowerCase.contains("record.name"))
  }

  it should "throw on record namespace not available if record.name naming strategy" in {
    val settings = schemaRegistrySettings ++ Map[String,String](
      KEY_SCHEMA_REGISTRY_URL -> schemaRegistryURL,
      KEY_SCHEMA_REGISTRY_VALUE_NAMING_STRATEGY -> SchemaStorageNamingStrategies.RECORD_NAME,
      KEY_SCHEMA_REGISTRY_VALUE_RECORD_NAME -> "any.name"
    )

    stubTopic()
    stubSchemaRegistrySettings(settings)

    val throwable = intercept[IllegalArgumentException](ConfluentAvroKafkaStreamDecoder(configStub))
    assert(throwable.getMessage.toLowerCase.contains("record.namespace"))
  }

  it should "throw on record name not available if topic.record.name naming strategy" in {
    val settings = schemaRegistrySettings ++ Map[String,String](
      KEY_SCHEMA_REGISTRY_URL -> schemaRegistryURL,
      KEY_SCHEMA_REGISTRY_VALUE_NAMING_STRATEGY -> SchemaStorageNamingStrategies.TOPIC_RECORD_NAME
    )

    stubTopic()
    stubSchemaRegistrySettings(settings)

    val throwable = intercept[IllegalArgumentException](ConfluentAvroKafkaStreamDecoder(configStub))
    assert(throwable.getMessage.toLowerCase.contains("record.name"))
  }

  it should "throw on record namespace not available if topic.record.name naming strategy" in {
    val settings = schemaRegistrySettings ++ Map[String,String](
      KEY_SCHEMA_REGISTRY_URL -> schemaRegistryURL,
      KEY_SCHEMA_REGISTRY_VALUE_NAMING_STRATEGY -> SchemaStorageNamingStrategies.TOPIC_RECORD_NAME,
      KEY_SCHEMA_REGISTRY_VALUE_RECORD_NAME -> "any.name"
    )

    stubTopic()
    stubSchemaRegistrySettings(settings)

    val throwable = intercept[IllegalArgumentException](ConfluentAvroKafkaStreamDecoder(configStub))
    assert(throwable.getMessage.toLowerCase.contains("record.namespace"))
  }

  it should "create avro stream decoder instance with topic naming strategy" in {
    stubTopic()
    stubSchemaRegistrySettings(schemaRegistrySettings)

    val decoder = ConfluentAvroKafkaStreamDecoder(configStub).asInstanceOf[ConfluentAvroKafkaStreamDecoder]

    assert(topic == decoder.topic)
    assert(schemaRegistrySettings(KEY_SCHEMA_REGISTRY_URL) == decoder.schemaRegistrySettings(PARAM_SCHEMA_REGISTRY_URL))
    assert(schemaRegistrySettings(KEY_SCHEMA_REGISTRY_VALUE_SCHEMA_ID) == decoder.schemaRegistrySettings(PARAM_VALUE_SCHEMA_ID))
    assert(schemaRegistrySettings(KEY_SCHEMA_REGISTRY_VALUE_NAMING_STRATEGY) == decoder.schemaRegistrySettings(PARAM_VALUE_SCHEMA_NAMING_STRATEGY))
  }

  it should "create avro stream decoder instance with record name naming strategy" in {
    val settings = schemaRegistrySettings ++ Map[String,String](
      KEY_SCHEMA_REGISTRY_URL -> schemaRegistryURL,
      KEY_SCHEMA_REGISTRY_VALUE_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.RECORD_NAME,
      KEY_SCHEMA_REGISTRY_VALUE_RECORD_NAME -> "any.name",
      KEY_SCHEMA_REGISTRY_VALUE_RECORD_NAMESPACE -> "any.namespace"
    )

    stubTopic()
    stubSchemaRegistrySettings(settings)

    val decoder = ConfluentAvroKafkaStreamDecoder(configStub).asInstanceOf[ConfluentAvroKafkaStreamDecoder]

    assert(topic == decoder.topic)
    assert(settings(KEY_SCHEMA_REGISTRY_URL) == decoder.schemaRegistrySettings(PARAM_SCHEMA_REGISTRY_URL))
    assert(settings(KEY_SCHEMA_REGISTRY_VALUE_SCHEMA_ID) == decoder.schemaRegistrySettings(PARAM_VALUE_SCHEMA_ID))
    assert(settings(KEY_SCHEMA_REGISTRY_VALUE_NAMING_STRATEGY) == decoder.schemaRegistrySettings(PARAM_VALUE_SCHEMA_NAMING_STRATEGY))
    assert(settings(KEY_SCHEMA_REGISTRY_VALUE_RECORD_NAME) == decoder.schemaRegistrySettings(PARAM_SCHEMA_NAME_FOR_RECORD_STRATEGY))
    assert(settings(KEY_SCHEMA_REGISTRY_VALUE_RECORD_NAMESPACE) == decoder.schemaRegistrySettings(PARAM_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY))
  }

  it should "create avro stream decoder instance with topic record name naming strategy" in {
    val settings = schemaRegistrySettings ++ Map[String,String](
      KEY_SCHEMA_REGISTRY_URL -> schemaRegistryURL,
      KEY_SCHEMA_REGISTRY_VALUE_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_RECORD_NAME,
      KEY_SCHEMA_REGISTRY_VALUE_RECORD_NAME -> "any.name",
      KEY_SCHEMA_REGISTRY_VALUE_RECORD_NAMESPACE -> "any.namespace"
    )

    stubTopic()
    stubSchemaRegistrySettings(settings)

    val decoder = ConfluentAvroKafkaStreamDecoder(configStub).asInstanceOf[ConfluentAvroKafkaStreamDecoder]

    assert(topic == decoder.topic)
    assert(settings(KEY_SCHEMA_REGISTRY_URL) == decoder.schemaRegistrySettings(PARAM_SCHEMA_REGISTRY_URL))
    assert(settings(KEY_SCHEMA_REGISTRY_VALUE_SCHEMA_ID) == decoder.schemaRegistrySettings(PARAM_VALUE_SCHEMA_ID))
    assert(settings(KEY_SCHEMA_REGISTRY_VALUE_NAMING_STRATEGY) == decoder.schemaRegistrySettings(PARAM_VALUE_SCHEMA_NAMING_STRATEGY))
    assert(settings(KEY_SCHEMA_REGISTRY_VALUE_RECORD_NAME) == decoder.schemaRegistrySettings(PARAM_SCHEMA_NAME_FOR_RECORD_STRATEGY))
    assert(settings(KEY_SCHEMA_REGISTRY_VALUE_RECORD_NAMESPACE) == decoder.schemaRegistrySettings(PARAM_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY))
  }

  private def stubProperty(key: String, value: String): Unit = when(configStub.getString(key)).thenReturn(value)

  private def stubTopic(): Unit = stubProperty(KEY_TOPIC, topic)

  private def stubSchemaRegistrySettings(settings: Map[String,String], keysToExclude: Set[String] = Set[String]()): Unit = {
    settings
      .filterKeys(!keysToExclude.contains(_))
      .foreach {case(key,value) => stubProperty(key, value)}
  }
}
