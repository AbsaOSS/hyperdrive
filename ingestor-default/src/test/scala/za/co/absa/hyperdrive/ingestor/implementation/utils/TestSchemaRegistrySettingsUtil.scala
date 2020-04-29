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

package za.co.absa.hyperdrive.ingestor.implementation.utils

import org.apache.commons.configuration2.BaseConfiguration
import org.scalatest.{FlatSpec, Matchers}
import za.co.absa.abris.avro.read.confluent.SchemaManager.SchemaStorageNamingStrategies.{RECORD_NAME, TOPIC_RECORD_NAME}
import za.co.absa.abris.avro.read.confluent.SchemaManager._

class TestSchemaRegistrySettingsUtil extends FlatSpec with Matchers {

  behavior of SchemaRegistrySettingsUtil.getClass.getName

  private val topic = "topic"
  private val schemaRegistryURL = "http://localhost:8081"
  private val schemaRegistryValueNamingStrategy = "topic.name"
  private val schemaRegistryValueSchemaId = "latest"

  private val keySchemaRegistryUrl = "schema.registry.url"
  private val keySchemaRegistryValueSchemaId = "schema.registry.value.schema.id"
  private val keySchemaRegistryValueNamingStrategy = "schema.registry.value.naming.strategy"
  private val keySchemaRegistryValueRecordName = "schema.registry.value.record.name"
  private val keySchemaRegistryValueRecordNamespace = "schema.registry.value.record.namespace"

  private object ValueSchemaProducerConfigKeys extends SchemaRegistryProducerConfigKeys {
    override val schemaRegistryUrl: String = keySchemaRegistryUrl
    override val namingStrategy: String = keySchemaRegistryValueNamingStrategy
    override val recordName: String = keySchemaRegistryValueRecordName
    override val recordNamespace: String = keySchemaRegistryValueRecordNamespace
    override val paramSchemaId: String = PARAM_VALUE_SCHEMA_ID
    override val paramSchemaNamingStrategy: String = PARAM_VALUE_SCHEMA_NAMING_STRATEGY
  }

  private object ValueSchemaConsumerConfigKeys extends SchemaRegistryConsumerConfigKeys {
    override val schemaRegistryUrl: String = keySchemaRegistryUrl
    override val schemaId: String = keySchemaRegistryValueSchemaId
    override val namingStrategy: String = keySchemaRegistryValueNamingStrategy
    override val recordName: String = keySchemaRegistryValueRecordName
    override val recordNamespace: String = keySchemaRegistryValueRecordNamespace
    override val paramSchemaId: String = PARAM_VALUE_SCHEMA_ID
    override val paramSchemaNamingStrategy: String = PARAM_VALUE_SCHEMA_NAMING_STRATEGY
  }

  "getProducerSettings" should "return settings for topic name strategy" in {
    val config = createBaseConfiguration
    config.addProperty(keySchemaRegistryValueNamingStrategy, schemaRegistryValueNamingStrategy)

    val settings = SchemaRegistrySettingsUtil.getProducerSettings(config, topic, ValueSchemaProducerConfigKeys)

    settings should contain theSameElementsAs Map(
      PARAM_SCHEMA_REGISTRY_TOPIC -> topic,
      PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> schemaRegistryValueNamingStrategy,
      PARAM_SCHEMA_REGISTRY_URL -> schemaRegistryURL
    )
  }

  "getConsumerSettings" should "return settings for topic name strategy" in {
    val config = createBaseConfiguration
    config.addProperty(keySchemaRegistryValueNamingStrategy, schemaRegistryValueNamingStrategy)

    val settings = SchemaRegistrySettingsUtil.getConsumerSettings(config, topic, ValueSchemaConsumerConfigKeys)

    settings should contain theSameElementsAs Map(
      PARAM_SCHEMA_REGISTRY_TOPIC -> topic,
      PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> schemaRegistryValueNamingStrategy,
      PARAM_SCHEMA_REGISTRY_URL -> schemaRegistryURL,
      PARAM_VALUE_SCHEMA_ID -> schemaRegistryValueSchemaId
    )
  }

  it should "return settings for topic record name strategy" in {
    val config = createBaseConfiguration
    config.addProperty(keySchemaRegistryValueNamingStrategy, TOPIC_RECORD_NAME)
    config.addProperty(keySchemaRegistryValueRecordName, "any.name")
    config.addProperty(keySchemaRegistryValueRecordNamespace, "any.namespace")

    val settings = SchemaRegistrySettingsUtil.getConsumerSettings(config, topic, ValueSchemaConsumerConfigKeys)

    settings should contain theSameElementsAs Map(
      PARAM_SCHEMA_REGISTRY_TOPIC -> topic,
      PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> TOPIC_RECORD_NAME,
      PARAM_SCHEMA_REGISTRY_URL -> schemaRegistryURL,
      PARAM_VALUE_SCHEMA_ID -> schemaRegistryValueSchemaId,
      PARAM_SCHEMA_NAME_FOR_RECORD_STRATEGY -> "any.name",
      PARAM_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY -> "any.namespace"
    )
  }

  it should "return settings for record name strategy" in {
    val config = createBaseConfiguration
    config.addProperty(keySchemaRegistryValueNamingStrategy, RECORD_NAME)
    config.addProperty(keySchemaRegistryValueRecordName, "any.name")
    config.addProperty(keySchemaRegistryValueRecordNamespace, "any.namespace")

    val settings = SchemaRegistrySettingsUtil.getConsumerSettings(config, topic, ValueSchemaConsumerConfigKeys)

    settings should contain theSameElementsAs Map(
      PARAM_SCHEMA_REGISTRY_TOPIC -> topic,
      PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> RECORD_NAME,
      PARAM_SCHEMA_REGISTRY_URL -> schemaRegistryURL,
      PARAM_VALUE_SCHEMA_ID -> schemaRegistryValueSchemaId,
      PARAM_SCHEMA_NAME_FOR_RECORD_STRATEGY -> "any.name",
      PARAM_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY -> "any.namespace"
    )
  }

  it should "throw an exception if schema id is not configured" in {
    val config = new BaseConfiguration
    config.addProperty(keySchemaRegistryUrl, schemaRegistryURL)
    config.addProperty(keySchemaRegistryValueNamingStrategy, schemaRegistryValueNamingStrategy)

    val exception = intercept[IllegalArgumentException](SchemaRegistrySettingsUtil.getConsumerSettings(config, topic, ValueSchemaConsumerConfigKeys))

    exception.getMessage should include(keySchemaRegistryValueSchemaId)
  }

  it should "throw an exception if schema registry url is not configured" in {
    val config = new BaseConfiguration
    config.addProperty(keySchemaRegistryValueSchemaId, schemaRegistryValueSchemaId)
    config.addProperty(keySchemaRegistryValueNamingStrategy, schemaRegistryValueNamingStrategy)

    val exception = intercept[IllegalArgumentException](SchemaRegistrySettingsUtil.getConsumerSettings(config, topic, ValueSchemaConsumerConfigKeys))

    exception.getMessage should include(keySchemaRegistryUrl)
  }

  it should "throw an exception if value naming strategy is not configured" in {
    val config = new BaseConfiguration
    config.addProperty(keySchemaRegistryUrl, schemaRegistryURL)
    config.addProperty(keySchemaRegistryValueSchemaId, schemaRegistryValueSchemaId)

    val exception = intercept[IllegalArgumentException](SchemaRegistrySettingsUtil.getConsumerSettings(config, topic, ValueSchemaConsumerConfigKeys))

    exception.getMessage should include(keySchemaRegistryValueNamingStrategy)
  }

  it should "throw an exception if record name is missing for topic record strategy" in {
    val config = createBaseConfiguration
    config.addProperty(keySchemaRegistryValueRecordNamespace, "any.namespace")
    config.addProperty(keySchemaRegistryValueNamingStrategy, TOPIC_RECORD_NAME)

    val exception = intercept[IllegalArgumentException](SchemaRegistrySettingsUtil.getConsumerSettings(config, topic, ValueSchemaConsumerConfigKeys))

    exception.getMessage should include(keySchemaRegistryValueRecordName)
  }

  it should "throw an exception if record name is missing for record strategy" in {
    val config = createBaseConfiguration
    config.addProperty(keySchemaRegistryValueRecordNamespace, "any.namespace")
    config.addProperty(keySchemaRegistryValueNamingStrategy, RECORD_NAME)

    val exception = intercept[IllegalArgumentException](SchemaRegistrySettingsUtil.getConsumerSettings(config, topic, ValueSchemaConsumerConfigKeys))

    exception.getMessage should include(keySchemaRegistryValueRecordName)
  }

  it should "throw an exception if record namespace is missing for topic record strategy" in {
    val config = createBaseConfiguration
    config.addProperty(keySchemaRegistryValueRecordName, "any.name")
    config.addProperty(keySchemaRegistryValueNamingStrategy, TOPIC_RECORD_NAME)

    val exception = intercept[IllegalArgumentException](SchemaRegistrySettingsUtil.getConsumerSettings(config, topic, ValueSchemaConsumerConfigKeys))

    exception.getMessage should include(keySchemaRegistryValueRecordNamespace)
  }

  it should "throw an exception if record namespace is missing for record strategy" in {
    val config = createBaseConfiguration
    config.addProperty(keySchemaRegistryValueRecordName, "any.name")
    config.addProperty(keySchemaRegistryValueNamingStrategy, RECORD_NAME)

    val exception = intercept[IllegalArgumentException](SchemaRegistrySettingsUtil.getConsumerSettings(config, topic, ValueSchemaConsumerConfigKeys))

    exception.getMessage should include(keySchemaRegistryValueRecordNamespace)
  }

  private def createBaseConfiguration = {
    val config = new BaseConfiguration
    config.addProperty(keySchemaRegistryUrl, schemaRegistryURL)
    config.addProperty(keySchemaRegistryValueSchemaId, schemaRegistryValueSchemaId)
    config
  }

}
