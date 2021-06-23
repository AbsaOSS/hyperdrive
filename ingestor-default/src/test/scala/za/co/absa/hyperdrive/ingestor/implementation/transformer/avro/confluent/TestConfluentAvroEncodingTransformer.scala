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
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.avro.read.confluent.SchemaManagerFactory
import za.co.absa.abris.config.AbrisConfig
import za.co.absa.commons.spark.SparkTestBase
import za.co.absa.hyperdrive.ingestor.api.context.HyperdriveContext
import za.co.absa.hyperdrive.ingestor.implementation.HyperdriveContextKeys
import za.co.absa.hyperdrive.ingestor.implementation.testutils.HyperdriveMockSchemaRegistryClient
import za.co.absa.hyperdrive.ingestor.implementation.transformer.avro.confluent.ConfluentAvroEncodingTransformer._
import za.co.absa.hyperdrive.ingestor.implementation.utils.AbrisConfigUtil
import za.co.absa.hyperdrive.ingestor.implementation.writer.kafka.KafkaStreamWriter

class TestConfluentAvroEncodingTransformer extends FlatSpec with Matchers with BeforeAndAfter with SparkTestBase {

  private val topic = "topic"
  private val SchemaRegistryURL = "http://localhost:8081"
  private var mockSchemaRegistryClient: MockSchemaRegistryClient = _
  behavior of ConfluentAvroEncodingTransformer.getClass.getSimpleName

  before {
    mockSchemaRegistryClient = new HyperdriveMockSchemaRegistryClient()
    SchemaManagerFactory.resetSRClientInstance()
    SchemaManagerFactory.addSRClientInstance(Map(AbrisConfig.SCHEMA_REGISTRY_URL -> SchemaRegistryURL), mockSchemaRegistryClient)
  }

  it should "create avro stream encoder" in {
    val config = new BaseConfiguration
    config.addProperty(KafkaStreamWriter.KEY_TOPIC, topic)
    config.addProperty(KEY_SCHEMA_REGISTRY_URL, SchemaRegistryURL)
    config.addProperty(KEY_SCHEMA_REGISTRY_VALUE_NAMING_STRATEGY, AbrisConfigUtil.TopicNameStrategy)

    val encoder = ConfluentAvroEncodingTransformer(config).asInstanceOf[ConfluentAvroEncodingTransformer]

    encoder.config shouldBe config
    encoder.withKey shouldBe false
  }

  "transform" should "encode the values" in {
    // given
    val schemaCatalyst = new StructType()
      .add("offset", LongType, nullable = true)
      .add("partition", IntegerType, nullable = true)
    val queryName = "dummyQuery"
    val offsets = (1 to 100).map(_ => 42L)
    val partitions = (1 to 100).map(_ % 5)

    val rows = offsets.zip(partitions).map(a => Row(a._1, a._2))
    val input = new MemoryStream[Row](1, spark.sqlContext)(RowEncoder(schemaCatalyst))
    input.addData(rows)
    val df = input.toDF()
      .filter(col("offset") ===  lit(42))
    df.printSchema()

    // when
    val config = new BaseConfiguration()
    config.addProperty(KafkaStreamWriter.KEY_TOPIC, topic)
    config.addProperty(KEY_SCHEMA_REGISTRY_URL, SchemaRegistryURL)
    config.addProperty(KEY_SCHEMA_REGISTRY_VALUE_NAMING_STRATEGY, AbrisConfigUtil.TopicNameStrategy)

    val encoder = ConfluentAvroEncodingTransformer(config)
    val transformedDf = encoder.transform(df)
    val query = transformedDf
      .writeStream
      .trigger(Trigger.Once)
      .queryName(queryName)
      .format("memory")
      .start()
    query.awaitTermination()

    // then
    import spark.implicits._
    val outputDf = spark.sql(s"select * from $queryName")
    outputDf.count() shouldBe 100
    val byteArrays = outputDf.select("value").map(_ (0).asInstanceOf[Array[Byte]]).collect()
    byteArrays.distinct.length shouldBe byteArrays.length
  }

  it should "register a schema with optional fields" in {
    // given
    val schema = StructType(Seq(
        StructField("key__col1", IntegerType, nullable = true),
        StructField("col2", StringType, nullable = true),
        StructField("col3", StructType(
          Seq(StructField("subCol1", StringType, nullable = true))
        ), nullable = true)
      )
    )
    HyperdriveContext.put(HyperdriveContextKeys.keyColumnPrefix, "key__")
    HyperdriveContext.put(HyperdriveContextKeys.keyColumnNames, Seq("col1"))
    val memoryStream = new MemoryStream[Row](1, spark.sqlContext)(RowEncoder(schema))

    val config = new BaseConfiguration()
    config.setListDelimiterHandler(new DefaultListDelimiterHandler(','))
    config.addProperty(KafkaStreamWriter.KEY_TOPIC, topic)
    config.addProperty(KEY_SCHEMA_REGISTRY_URL, SchemaRegistryURL)
    config.addProperty(KEY_SCHEMA_REGISTRY_VALUE_NAMING_STRATEGY, AbrisConfigUtil.TopicNameStrategy)
    config.addProperty(KEY_PRODUCE_KEYS, "true")
    config.addProperty(KEY_KEY_OPTIONAL_FIELDS, "col1")
    config.addProperty(KEY_VALUE_OPTIONAL_FIELDS, "col2, col3, col3.subCol1")
    val encoder = ConfluentAvroEncodingTransformer(config)

    val expectedKeySchemaString = {
      raw"""{
           |  "type" : "record",
           |  "name" : "topLevelRecord",
           |  "fields" : [ {
           |    "name" : "col1",
           |    "type" : [ "null", "int" ],
           |    "default" : null
           |  } ]
           |}
           |""".stripMargin
    }
    val expectedKeySchema = AvroSchemaUtils.parse(expectedKeySchemaString)

    val expectedValueSchemaString =
      raw"""{
           |  "type" : "record",
           |  "name" : "topLevelRecord",
           |  "fields" : [ {
           |    "name" : "col2",
           |    "type" : [ "null", "string" ],
           |    "default" : null
           |  }, {
           |    "name" : "col3",
           |    "type" : [ "null", {
           |      "type" : "record",
           |      "name" : "col3",
           |      "namespace" : "topLevelRecord",
           |      "fields" : [ {
           |        "name" : "subCol1",
           |        "type" : [ "null", "string" ],
           |        "default" : null
           |      } ]
           |    } ],
           |    "default" : null
           |  } ]
           |}
           |""".stripMargin
    val expectedValueSchema = AvroSchemaUtils.parse(expectedValueSchemaString)

    // when
    encoder.transform(memoryStream.toDF())

    // then
    val keySchema = mockSchemaRegistryClient.getLatestSchemaMetadata(s"$topic-key")
    keySchema.getSchema shouldBe expectedKeySchema.toString
    val valueSchema = mockSchemaRegistryClient.getLatestSchemaMetadata(s"$topic-value")
    valueSchema.getSchema shouldBe expectedValueSchema.toString
  }
}
