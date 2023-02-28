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

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import org.apache.avro.JsonProperties
import org.apache.commons.configuration2.BaseConfiguration
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, IntegerType, MetadataBuilder, StringType}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.avro.read.confluent.SchemaManagerFactory
import za.co.absa.abris.avro.registry.ConfluentMockRegistryClient
import za.co.absa.abris.config.AbrisConfig
import za.co.absa.hyperdrive.ingestor.implementation.testutils.HyperdriveMockSchemaRegistryClient
import za.co.absa.abris.avro.sql.AbrisTestUtil.{getAbrisConfig, getFromSchemaString, getSchemaId, getSchemaRegistryConf}
import za.co.absa.hyperdrive.ingestor.implementation.transformer.avro.confluent.{AdvancedAvroToSparkConverter, AdvancedSparkToAvroConverter, SparkMetadataKeys}

class TestAbrisConfigUtil extends AnyFlatSpec with Matchers with BeforeAndAfter {

  import scala.collection.JavaConverters._

  private var mockSchemaRegistryClient: MockSchemaRegistryClient = _
  private val dummySchemaRegistryUrl = "http://localhost:8081"
  private val latestSchema = "latest"
  private val topic = "topic"
  private val recordName = "record_name"
  private val recordNamespace = "record_namespace"
  private val columnName = "column"

  private def getSchemaString(name: String, namespace: String) = {
    raw"""{
     "type": "record",
     "name": "$name",
     "namespace": "$namespace",
     "fields":[
         {"name": "$columnName", "type": ["int", "null"] }
     ]
    }"""
  }

  private val dummyRecordNameSchema = AvroSchemaUtils.parse(getSchemaString(recordName, recordNamespace))
  private val dummyTopicNameSchema = AvroSchemaUtils.parse(getSchemaString("topLevelRecord", ""))

  private val keyTopic = "kafka.topic"
  private val keySchemaRegistryUrl = "schema.registry.url"
  private val keySchemaRegistrySchemaId = "schema.registry.schema.id"
  private val keySchemaRegistryNamingStrategy = "schema.registry.naming.strategy"
  private val keySchemaRegistryRecordName = "schema.registry.record.name"
  private val keySchemaRegistryRecordNamespace = "schema.registry.record.namespace"
  private val keyUseAdvancedSchemaConversion = "use.advanced.schema.conversion"

  private object ProducerConfigKeys extends AbrisProducerConfigKeys {
    override val namingStrategy: String = keySchemaRegistryNamingStrategy
    override val recordName: String = keySchemaRegistryRecordName
    override val recordNamespace: String = keySchemaRegistryRecordNamespace
    override val topic: String = keyTopic
  }

  private object ConsumerConfigKeys extends AbrisConsumerConfigKeys {
    override val schemaId: String = keySchemaRegistrySchemaId
    override val namingStrategy: String = keySchemaRegistryNamingStrategy
    override val recordName: String = keySchemaRegistryRecordName
    override val recordNamespace: String = keySchemaRegistryRecordNamespace
    override val topic: String = keyTopic
    override val useAdvancedSchemaConversion: String = keyUseAdvancedSchemaConversion
  }


  behavior of AbrisConfigUtil.getClass.getName

  before {
    mockSchemaRegistryClient = new HyperdriveMockSchemaRegistryClient()
    SchemaManagerFactory.resetSRClientInstance()
    SchemaManagerFactory.addSRClientInstance(Map(AbrisConfig.SCHEMA_REGISTRY_URL -> dummySchemaRegistryUrl),
      new ConfluentMockRegistryClient(mockSchemaRegistryClient))
  }

  "generateSchema" should "generate a schema with topic name strategy" in {
    val config = createBaseConfiguration
    config.addProperty(keySchemaRegistryNamingStrategy, AbrisConfigUtil.TopicNameStrategy)
    val dummyExpr = struct(lit(null).cast(IntegerType).as(columnName)).expr

    val schema = AbrisConfigUtil.generateSchema(config, ProducerConfigKeys, dummyExpr, Map())

    schema shouldBe dummyTopicNameSchema
  }

  it should "generate a schema with record name strategy" in {
    val config = createBaseConfiguration
    config.addProperty(keySchemaRegistryNamingStrategy, AbrisConfigUtil.RecordNameStrategy)
    config.addProperty(keySchemaRegistryRecordName, recordName)
    config.addProperty(keySchemaRegistryRecordNamespace, recordNamespace)
    val dummyExpr = struct(lit(null).cast(IntegerType).as(columnName)).expr

    val schema = AbrisConfigUtil.generateSchema(config, ProducerConfigKeys, dummyExpr, Map())

    schema shouldBe dummyRecordNameSchema
  }

  it should "set default values for the specified fields" in {
    val config = createBaseConfiguration
    config.addProperty(keySchemaRegistryNamingStrategy, AbrisConfigUtil.TopicNameStrategy)
    val expression = struct(
      lit(null).cast(IntegerType).as("col1"),
      lit("abc").cast(StringType).as("col2"),
      struct(
        lit(null).cast(IntegerType).as("subcol1")
      ).as("col3"),
      array(struct(
        lit(null).cast(BooleanType).as("subcol1")
      )).as("col4"),
      map(
        lit("abc").as("keycol1"),
        struct(lit(null).cast(StringType).as("valuecol1")
        )).as("col5")
    ).expr

    val expectedSchemaString =
      raw"""{
           | "type": "record",
           | "name": "topLevelRecord",
           | "fields":[
           |     {"name": "col1", "type": ["int", "null"], "default": 42 },
           |     {"name": "col2", "type": "string"},
           |     {"name": "col3", "type":
           |        {"type": "record", "name": "col3", "namespace": "topLevelRecord", "fields":[
           |          {"name": "subcol1", "type": ["null", "int"], "default": null}]}},
           |     {"name": "col4", "type":
           |        { "type": "array", "items":
           |            { "type": "record", "name": "col4", "namespace": "topLevelRecord", "fields":[
           |                {"name": "subcol1", "type": ["null", "boolean"], "default": null}]}}},
           |     {"name": "col5", "type":
           |        { "type":"map", "values":
           |            { "type": "record", "name": "col5", "namespace":"topLevelRecord", "fields":[
           |                {"name":"valuecol1","type":["null","string"], "default": null}]}}}
           | ]
           |}
           |""".stripMargin
    val expectedSchema = AvroSchemaUtils.parse(expectedSchemaString)

    val schema = AbrisConfigUtil.generateSchema(config, ProducerConfigKeys, expression, Map(
      "col1" -> 42.asInstanceOf[Object],
      "col3.subcol1" -> JsonProperties.NULL_VALUE,
      "col4.subcol1" -> JsonProperties.NULL_VALUE,
      "col5.valuecol1" -> JsonProperties.NULL_VALUE
    ))

    schema shouldBe expectedSchema
  }

  it should "generate a schema with advanced spark avro conversion" in {
    val config = createBaseConfiguration
    config.addProperty(keySchemaRegistryNamingStrategy, AbrisConfigUtil.TopicNameStrategy)

    val nullValue = null
    val dummyExpr = struct(
      lit(nullValue)
        .cast(IntegerType)
        .as("col1", new MetadataBuilder().putString(SparkMetadataKeys.DefaultValueKey, "42").build())
    ).expr
    val expectedSchemaString = raw"""{
           |  "type" : "record",
           |  "name" : "topLevelRecord",
           |  "fields" : [ {
           |    "name" : "col1",
           |    "type" : [ "int", "null" ],
           |    "default" : 42
           |  } ]
           |}
           |""".stripMargin.filterNot(_.isWhitespace)

    val schema = AbrisConfigUtil.generateSchema(config, ProducerConfigKeys, dummyExpr, Map(),
      AdvancedSparkToAvroConverter)

    schema.toString shouldBe expectedSchemaString
  }

  "getKeyProducerSettings" should "return settings and register subject with topic name strategy" in {
    // given
    val config = createBaseConfiguration
    val schemaRegistryConfig = createBaseSchemaRegistryConfig
    config.addProperty(keyTopic, topic)
    config.addProperty(keySchemaRegistryNamingStrategy, AbrisConfigUtil.TopicNameStrategy)

    // when
    val settings = AbrisConfigUtil.getKeyProducerSettings(config, ProducerConfigKeys, dummyTopicNameSchema, schemaRegistryConfig)

    // then
    getSchemaId(settings) shouldBe Some(1)
    mockSchemaRegistryClient.getAllSubjects.asScala should contain theSameElementsAs Seq(s"${topic}-key")
  }

  it should "return settings and register subject with record name strategy" in {
    // given
    val config = createBaseConfiguration
    val schemaRegistryConfig = createBaseSchemaRegistryConfig
    config.addProperty(keyTopic, topic)
    config.addProperty(keySchemaRegistryNamingStrategy, AbrisConfigUtil.RecordNameStrategy)
    config.addProperty(keySchemaRegistryRecordName, recordName)
    config.addProperty(keySchemaRegistryRecordNamespace, recordNamespace)

    // when
    val settings = AbrisConfigUtil.getKeyProducerSettings(config, ProducerConfigKeys, dummyRecordNameSchema, schemaRegistryConfig)

    // then
    getSchemaId(settings) shouldBe Some(1)
    mockSchemaRegistryClient.getAllSubjects.asScala should contain theSameElementsAs Seq(s"$recordNamespace.$recordName")
  }

  it should "return settings and register subject with topic record name strategy" in {
    // given
    val config = createBaseConfiguration
    val schemaRegistryConfig = createBaseSchemaRegistryConfig
    config.addProperty(keyTopic, topic)
    config.addProperty(keySchemaRegistryNamingStrategy, AbrisConfigUtil.TopicRecordNameStrategy)
    config.addProperty(keySchemaRegistryRecordName, recordName)
    config.addProperty(keySchemaRegistryRecordNamespace, recordNamespace)

    // when
    val settings = AbrisConfigUtil.getKeyProducerSettings(config, ProducerConfigKeys, dummyRecordNameSchema, schemaRegistryConfig)

    // then
    getSchemaId(settings) shouldBe Some(1)
    mockSchemaRegistryClient.getAllSubjects.asScala should contain theSameElementsAs Seq(s"$topic-$recordNamespace.$recordName")
  }

  "getValueProducerSettings" should "return settings and register subject with topic name strategy" in {
    // given
    val config = createBaseConfiguration
    config.addProperty(keyTopic, topic)
    config.addProperty(keySchemaRegistryNamingStrategy, AbrisConfigUtil.TopicNameStrategy)
    val schemaRegistryConfig = createBaseSchemaRegistryConfig

    // when
    val settings = AbrisConfigUtil.getValueProducerSettings(config, ProducerConfigKeys, dummyTopicNameSchema, schemaRegistryConfig)

    // then
    getSchemaId(settings) shouldBe Some(1)
    mockSchemaRegistryClient.getAllSubjects.asScala should contain theSameElementsAs Seq(s"$topic-value")
  }

  "getKeyConsumerSettings" should "return settings and register subject with topic name strategy for latest schema" in {
    // given
    mockSchemaRegistryClient.register(s"$topic-key", dummyTopicNameSchema)
    val config = createBaseConfiguration
    config.addProperty(keyTopic, topic)
    config.addProperty(keySchemaRegistrySchemaId, latestSchema)
    config.addProperty(keySchemaRegistryNamingStrategy, AbrisConfigUtil.TopicNameStrategy)
    config.addProperty(keySchemaRegistryUrl, dummySchemaRegistryUrl)
    config.addProperty(keyUseAdvancedSchemaConversion, "true")

    val schemaRegistryConfig = createBaseSchemaRegistryConfig

    // when
    val settings = AbrisConfigUtil.getKeyConsumerSettings(config, ConsumerConfigKeys, schemaRegistryConfig)

    // then
    getFromSchemaString(settings) shouldBe dummyTopicNameSchema.toString
    getAbrisConfig(settings)("schemaConverter") shouldBe AdvancedAvroToSparkConverter.name
    getSchemaRegistryConf(settings).get shouldBe Map("schema.registry.url" -> dummySchemaRegistryUrl)
  }

  it should "return settings and register subject with topic name strategy for schema id" in {
    // given
    val schema2String = raw"""{
     "type": "record",
     "name": "topLevelRecord",
     "namespace": "",
     "fields":[
         {"name": "$columnName", "type": ["int", "null"] },
         {"name": "${columnName}2", "type": ["int", "null"] }
     ]
    }"""
    val schema2 = AvroSchemaUtils.parse(schema2String)
    mockSchemaRegistryClient.register(s"$topic-key", dummyTopicNameSchema)
    mockSchemaRegistryClient.register(s"$topic-key", schema2)
    val config = createBaseConfiguration
    config.addProperty(keyTopic, topic)
    config.addProperty(keySchemaRegistrySchemaId, 2)
    config.addProperty(keySchemaRegistryNamingStrategy, AbrisConfigUtil.TopicNameStrategy)
    val schemaRegistryConfig = createBaseSchemaRegistryConfig

    // when
    val settings = AbrisConfigUtil.getKeyConsumerSettings(config, ConsumerConfigKeys, schemaRegistryConfig)

    // then
    getFromSchemaString(settings) shouldBe schema2.toString
    getSchemaRegistryConf(settings).get shouldBe Map("schema.registry.url" -> dummySchemaRegistryUrl)
  }

  it should "return settings and register subject with record name strategy" in {
    // given
    mockSchemaRegistryClient.register(s"$recordNamespace.$recordName", dummyRecordNameSchema)
    val config = createBaseConfiguration
    config.addProperty(keyTopic, topic)
    config.addProperty(keySchemaRegistrySchemaId, latestSchema)
    config.addProperty(keySchemaRegistryRecordName, recordName)
    config.addProperty(keySchemaRegistryRecordNamespace, recordNamespace)
    config.addProperty(keySchemaRegistryNamingStrategy, AbrisConfigUtil.RecordNameStrategy)
    config.addProperty(keySchemaRegistryUrl, dummySchemaRegistryUrl)
    val schemaRegistryConfig = createBaseSchemaRegistryConfig

    // when
    val settings = AbrisConfigUtil.getKeyConsumerSettings(config, ConsumerConfigKeys, schemaRegistryConfig)

    // then
    getFromSchemaString(settings) shouldBe dummyRecordNameSchema.toString
    getSchemaRegistryConf(settings).get shouldBe Map("schema.registry.url" -> dummySchemaRegistryUrl)
  }

  it should "return settings and register subject with topic record name strategy" in {
    // given
    mockSchemaRegistryClient.register(s"$topic-$recordNamespace.$recordName", dummyRecordNameSchema)
    val config = createBaseConfiguration
    config.addProperty(keyTopic, topic)
    config.addProperty(keySchemaRegistrySchemaId, latestSchema)
    config.addProperty(keySchemaRegistryRecordName, recordName)
    config.addProperty(keySchemaRegistryRecordNamespace, recordNamespace)
    config.addProperty(keySchemaRegistryNamingStrategy, AbrisConfigUtil.TopicRecordNameStrategy)
    config.addProperty(keySchemaRegistryUrl, dummySchemaRegistryUrl)
    val schemaRegistryConfig = createBaseSchemaRegistryConfig

    // when
    val settings = AbrisConfigUtil.getKeyConsumerSettings(config, ConsumerConfigKeys, schemaRegistryConfig)

    // then
    getFromSchemaString(settings) shouldBe dummyRecordNameSchema.toString
    getSchemaRegistryConf(settings).get shouldBe Map("schema.registry.url" -> dummySchemaRegistryUrl)
  }

  "getValueConsumerSettings" should "return settings and register subject with topic name strategy for latest schema" in {
    // given
    mockSchemaRegistryClient.register(s"$topic-value", dummyTopicNameSchema)
    val config = createBaseConfiguration
    config.addProperty(keyTopic, topic)
    config.addProperty(keySchemaRegistrySchemaId, latestSchema)
    config.addProperty(keySchemaRegistryNamingStrategy, AbrisConfigUtil.TopicNameStrategy)
    config.addProperty(keySchemaRegistryUrl, dummySchemaRegistryUrl)
    val schemaRegistryConfig = createBaseSchemaRegistryConfig

    // when
    val settings = AbrisConfigUtil.getValueConsumerSettings(config, ConsumerConfigKeys, schemaRegistryConfig)

    // then
    getFromSchemaString(settings) shouldBe dummyTopicNameSchema.toString
    getSchemaRegistryConf(settings).get shouldBe Map("schema.registry.url" -> dummySchemaRegistryUrl)
  }

  it should "throw an exception if schema id is not configured" in {
    val config = new BaseConfiguration
    val schemaRegistryConfig = createBaseSchemaRegistryConfig

    val exception = intercept[IllegalArgumentException](AbrisConfigUtil.getValueConsumerSettings(config, ConsumerConfigKeys, schemaRegistryConfig))

    exception.getMessage should include(keySchemaRegistrySchemaId)
  }

  it should "throw an exception if topic is not configured" in {
    val config = new BaseConfiguration
    config.addProperty(keySchemaRegistrySchemaId, latestSchema)
    config.addProperty(keySchemaRegistryNamingStrategy, AbrisConfigUtil.TopicRecordNameStrategy)
    val schemaRegistryConfig = createBaseSchemaRegistryConfig

    val exception = intercept[IllegalArgumentException](AbrisConfigUtil.getValueConsumerSettings(config, ConsumerConfigKeys, schemaRegistryConfig))

    exception.getMessage should include(keyTopic)
  }

  it should "throw an exception if schema registry url is not configured" in {
    val config = new BaseConfiguration
    config.addProperty(keyTopic, topic)
    config.addProperty(keySchemaRegistrySchemaId, latestSchema)
    config.addProperty(keySchemaRegistryNamingStrategy, AbrisConfigUtil.TopicNameStrategy)

    val exception = intercept[Exception](AbrisConfigUtil.getValueConsumerSettings(config, ConsumerConfigKeys, Map()))

    exception.getMessage should include(keySchemaRegistryUrl)
  }

  it should "throw an exception if naming strategy is not configured" in {
    val config = new BaseConfiguration
    config.addProperty(keyTopic, topic)
    config.addProperty(keySchemaRegistrySchemaId, latestSchema)
    config.addProperty(keySchemaRegistryUrl, dummySchemaRegistryUrl)
    val schemaRegistryConfig = createBaseSchemaRegistryConfig

    val exception = intercept[IllegalArgumentException](AbrisConfigUtil.getValueConsumerSettings(config, ConsumerConfigKeys, schemaRegistryConfig))

    exception.getMessage should include(keySchemaRegistryNamingStrategy)
  }

  it should "throw an exception if record name is missing for record name strategy" in {
    val config = createBaseConfiguration
    config.addProperty(keyTopic, topic)
    config.addProperty(keySchemaRegistrySchemaId, latestSchema)
    config.addProperty(keySchemaRegistryNamingStrategy, AbrisConfigUtil.RecordNameStrategy)
    config.addProperty(keySchemaRegistryRecordNamespace, "any.namespace")
    val schemaRegistryConfig = createBaseSchemaRegistryConfig

    val exception = intercept[IllegalArgumentException](AbrisConfigUtil.getValueConsumerSettings(config, ConsumerConfigKeys, schemaRegistryConfig))

    exception.getMessage should include(keySchemaRegistryRecordName)
  }

  it should "throw an exception if record namespace is missing for record name strategy" in {
    val config = createBaseConfiguration
    config.addProperty(keyTopic, topic)
    config.addProperty(keySchemaRegistrySchemaId, latestSchema)
    config.addProperty(keySchemaRegistryNamingStrategy, AbrisConfigUtil.RecordNameStrategy)
    config.addProperty(keySchemaRegistryRecordName, "any.name")
    val schemaRegistryConfig = createBaseSchemaRegistryConfig

    val exception = intercept[IllegalArgumentException](AbrisConfigUtil.getValueConsumerSettings(config, ConsumerConfigKeys, schemaRegistryConfig))

    exception.getMessage should include(keySchemaRegistryRecordNamespace)
  }

  private def createBaseConfiguration = {
    val config = new BaseConfiguration
    config.addProperty(keySchemaRegistrySchemaId, latestSchema)
    config
  }

  private def createBaseSchemaRegistryConfig =
    Map(AbrisConfig.SCHEMA_REGISTRY_URL -> dummySchemaRegistryUrl)
}
