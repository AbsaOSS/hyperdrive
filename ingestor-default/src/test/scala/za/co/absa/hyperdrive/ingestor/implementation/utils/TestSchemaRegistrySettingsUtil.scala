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
import za.co.absa.hyperdrive.ingestor.implementation.utils.TestSchemaRegistrySettingsUtil._

class TestSchemaRegistrySettingsUtil extends FlatSpec with Matchers {

  behavior of SchemaRegistrySettingsUtil.getClass.getName

  "getProducerSettings" should "return settings for topic name strategy" in {
    val config = createBaseConfiguration
    config.addProperty(KeySchemaRegistryValueNamingStrategy, SchemaRegistryValueNamingStrategy)

    val settings = SchemaRegistrySettingsUtil.getProducerSettings(config, Topic, ValueSchemaProducerConfigKeys)

    settings should contain theSameElementsAs Map(
      PARAM_SCHEMA_REGISTRY_TOPIC -> Topic,
      PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> SchemaRegistryValueNamingStrategy,
      PARAM_SCHEMA_REGISTRY_URL -> SchemaRegistryURL
    )
  }

  "getConsumerSettings" should "return settings for topic name strategy" in {
    val config = createBaseConfiguration
    config.addProperty(KeySchemaRegistryValueNamingStrategy, SchemaRegistryValueNamingStrategy)

    val settings = SchemaRegistrySettingsUtil.getConsumerSettings(config, Topic, ValueSchemaConsumerConfigKeys)

    settings should contain theSameElementsAs Map(
      PARAM_SCHEMA_REGISTRY_TOPIC -> Topic,
      PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> SchemaRegistryValueNamingStrategy,
      PARAM_SCHEMA_REGISTRY_URL -> SchemaRegistryURL,
      PARAM_VALUE_SCHEMA_ID -> SchemaRegistryValueSchemaId
    )
  }

  it should "return settings for topic record name strategy" in {
    val config = createBaseConfiguration
    config.addProperty(KeySchemaRegistryValueNamingStrategy, TOPIC_RECORD_NAME)
    config.addProperty(KeySchemaRegistryValueRecordName, "any.name")
    config.addProperty(KeySchemaRegistryValueRecordNamespace, "any.namespace")

    val settings = SchemaRegistrySettingsUtil.getConsumerSettings(config, Topic, ValueSchemaConsumerConfigKeys)

    settings should contain theSameElementsAs Map(
      PARAM_SCHEMA_REGISTRY_TOPIC -> Topic,
      PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> TOPIC_RECORD_NAME,
      PARAM_SCHEMA_REGISTRY_URL -> SchemaRegistryURL,
      PARAM_VALUE_SCHEMA_ID -> SchemaRegistryValueSchemaId,
      PARAM_SCHEMA_NAME_FOR_RECORD_STRATEGY -> "any.name",
      PARAM_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY -> "any.namespace"
    )
  }

  it should "return settings for record name strategy" in {
    val config = createBaseConfiguration
    config.addProperty(KeySchemaRegistryValueNamingStrategy, RECORD_NAME)
    config.addProperty(KeySchemaRegistryValueRecordName, "any.name")
    config.addProperty(KeySchemaRegistryValueRecordNamespace, "any.namespace")

    val settings = SchemaRegistrySettingsUtil.getConsumerSettings(config, Topic, ValueSchemaConsumerConfigKeys)

    settings should contain theSameElementsAs Map(
      PARAM_SCHEMA_REGISTRY_TOPIC -> Topic,
      PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> RECORD_NAME,
      PARAM_SCHEMA_REGISTRY_URL -> SchemaRegistryURL,
      PARAM_VALUE_SCHEMA_ID -> SchemaRegistryValueSchemaId,
      PARAM_SCHEMA_NAME_FOR_RECORD_STRATEGY -> "any.name",
      PARAM_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY -> "any.namespace"
    )
  }

  it should "throw an exception if schema id is not configured" in {
    val config = new BaseConfiguration
    config.addProperty(KeySchemaRegistryUrl, SchemaRegistryURL)
    config.addProperty(KeySchemaRegistryValueNamingStrategy, SchemaRegistryValueNamingStrategy)

    val exception = intercept[IllegalArgumentException](SchemaRegistrySettingsUtil.getConsumerSettings(config, Topic, ValueSchemaConsumerConfigKeys))

    exception.getMessage should include(KeySchemaRegistryValueSchemaId)
  }

  it should "throw an exception if schema registry url is not configured" in {
    val config = new BaseConfiguration
    config.addProperty(KeySchemaRegistryValueSchemaId, SchemaRegistryValueSchemaId)
    config.addProperty(KeySchemaRegistryValueNamingStrategy, SchemaRegistryValueNamingStrategy)

    val exception = intercept[IllegalArgumentException](SchemaRegistrySettingsUtil.getConsumerSettings(config, Topic, ValueSchemaConsumerConfigKeys))

    exception.getMessage should include(KeySchemaRegistryUrl)
  }

  it should "throw an exception if value naming strategy is not configured" in {
    val config = new BaseConfiguration
    config.addProperty(KeySchemaRegistryUrl, SchemaRegistryURL)
    config.addProperty(KeySchemaRegistryValueSchemaId, SchemaRegistryValueSchemaId)

    val exception = intercept[IllegalArgumentException](SchemaRegistrySettingsUtil.getConsumerSettings(config, Topic, ValueSchemaConsumerConfigKeys))

    exception.getMessage should include(KeySchemaRegistryValueNamingStrategy)
  }

  it should "throw an exception if record name is missing for topic record strategy" in {
    val config = createBaseConfiguration
    config.addProperty(KeySchemaRegistryValueRecordNamespace, "any.namespace")
    config.addProperty(KeySchemaRegistryValueNamingStrategy, TOPIC_RECORD_NAME)

    val exception = intercept[IllegalArgumentException](SchemaRegistrySettingsUtil.getConsumerSettings(config, Topic, ValueSchemaConsumerConfigKeys))

    exception.getMessage should include(KeySchemaRegistryValueRecordName)
  }

  it should "throw an exception if record name is missing for record strategy" in {
    val config = createBaseConfiguration
    config.addProperty(KeySchemaRegistryValueRecordNamespace, "any.namespace")
    config.addProperty(KeySchemaRegistryValueNamingStrategy, RECORD_NAME)

    val exception = intercept[IllegalArgumentException](SchemaRegistrySettingsUtil.getConsumerSettings(config, Topic, ValueSchemaConsumerConfigKeys))

    exception.getMessage should include(KeySchemaRegistryValueRecordName)
  }

  it should "throw an exception if record namespace is missing for topic record strategy" in {
    val config = createBaseConfiguration
    config.addProperty(KeySchemaRegistryValueRecordName, "any.name")
    config.addProperty(KeySchemaRegistryValueNamingStrategy, TOPIC_RECORD_NAME)

    val exception = intercept[IllegalArgumentException](SchemaRegistrySettingsUtil.getConsumerSettings(config, Topic, ValueSchemaConsumerConfigKeys))

    exception.getMessage should include(KeySchemaRegistryValueRecordNamespace)
  }

  it should "throw an exception if record namespace is missing for record strategy" in {
    val config = createBaseConfiguration
    config.addProperty(KeySchemaRegistryValueRecordName, "any.name")
    config.addProperty(KeySchemaRegistryValueNamingStrategy, RECORD_NAME)

    val exception = intercept[IllegalArgumentException](SchemaRegistrySettingsUtil.getConsumerSettings(config, Topic, ValueSchemaConsumerConfigKeys))

    exception.getMessage should include(KeySchemaRegistryValueRecordNamespace)
  }

  private def createBaseConfiguration = {
    val config = new BaseConfiguration
    config.addProperty(KeySchemaRegistryUrl, SchemaRegistryURL)
    config.addProperty(KeySchemaRegistryValueSchemaId, SchemaRegistryValueSchemaId)
    config
  }

}

object TestSchemaRegistrySettingsUtil {

  private val Topic = "topic"
  private val SchemaRegistryURL = "http://localhost:8081"
  private val SchemaRegistryValueNamingStrategy = "topic.name"
  private val SchemaRegistryValueSchemaId = "latest"

  private val KeySchemaRegistryUrl = "schema.registry.url"
  private val KeySchemaRegistryValueSchemaId = "schema.registry.value.schema.id"
  private val KeySchemaRegistryValueNamingStrategy = "schema.registry.value.naming.strategy"
  private val KeySchemaRegistryValueRecordName = "schema.registry.value.record.name"
  private val KeySchemaRegistryValueRecordNamespace = "schema.registry.value.record.namespace"

  private object ValueSchemaProducerConfigKeys extends SchemaRegistryProducerConfigKeys {
    override val schemaRegistryUrl: String = KeySchemaRegistryUrl
    override val namingStrategy: String = KeySchemaRegistryValueNamingStrategy
    override val recordName: String = KeySchemaRegistryValueRecordName
    override val recordNamespace: String = KeySchemaRegistryValueRecordNamespace
    override val paramSchemaId: String = PARAM_VALUE_SCHEMA_ID
    override val paramSchemaNamingStrategy: String = PARAM_VALUE_SCHEMA_NAMING_STRATEGY
  }

  private object ValueSchemaConsumerConfigKeys extends SchemaRegistryConsumerConfigKeys {
    override val schemaRegistryUrl: String = KeySchemaRegistryUrl
    override val schemaId: String = KeySchemaRegistryValueSchemaId
    override val namingStrategy: String = KeySchemaRegistryValueNamingStrategy
    override val recordName: String = KeySchemaRegistryValueRecordName
    override val recordNamespace: String = KeySchemaRegistryValueRecordNamespace
    override val paramSchemaId: String = PARAM_VALUE_SCHEMA_ID
    override val paramSchemaNamingStrategy: String = PARAM_VALUE_SCHEMA_NAMING_STRATEGY
  }

}
