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
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.generic.GenericData
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler
import org.apache.commons.configuration2.{BaseConfiguration, DynamicCombinedConfiguration}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{BinaryType, IntegerType, LongType, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.avro.read.confluent.SchemaManagerFactory
import za.co.absa.abris.avro.registry.ConfluentMockRegistryClient
import za.co.absa.abris.config.AbrisConfig
import za.co.absa.spark.commons.test.SparkTestBase
import za.co.absa.hyperdrive.ingestor.implementation.reader.kafka.KafkaStreamReader.KEY_TOPIC
import za.co.absa.abris.avro.sql.AbrisTestUtil.getSchemaRegistryConf
import za.co.absa.hyperdrive.ingestor.implementation.transformer.avro.confluent.ConfluentAvroDecodingTransformer._
import za.co.absa.hyperdrive.ingestor.implementation.transformer.avro.confluent.ConfluentAvroEncodingTransformer.{KEY_SCHEMA_REGISTRY_URL, KEY_SCHEMA_REGISTRY_VALUE_NAMING_STRATEGY}
import za.co.absa.hyperdrive.ingestor.implementation.utils.AbrisConfigUtil

class TestConfluentAvroDecodingTransformer extends AnyFlatSpec with Matchers with BeforeAndAfter with SparkTestBase {

  private val Topic = "topic"
  private val SchemaRegistryURL = "dummyUrl"
  private val SchemaRegistryValueSchemaId = "latest"

  private var MockSchemaRegistryClient: MockSchemaRegistryClient = _
  private val DummySchema = AvroSchemaUtils.parse(
    """{
     "type": "record",
     "name": "default_name",
     "namespace": "default_namespace",
     "fields":[
         {"name": "int", "type":  ["int", "null"] }
     ]
    }""")

  private val ValueSchemaAvro = AvroSchemaUtils.parse(
    """{
     "type": "record",
     "name": "default_name",
     "namespace": "default_namespace",
     "fields":[
         {"name": "col1", "type":  ["int", "null"] },
         {"name": "col2", "type":  "string" }
     ]
    }""")

  private val KeySchemaAvro = AvroSchemaUtils.parse(
    """{
     "type": "record",
     "name": "default_name",
     "namespace": "default_namespace",
     "fields":[
         {"name": "col1", "type":  "long"}
     ]
    }""")

  behavior of ConfluentAvroDecodingTransformer.getClass.getSimpleName

  before {
    MockSchemaRegistryClient = new MockSchemaRegistryClient()

    SchemaManagerFactory.resetSRClientInstance()
    SchemaManagerFactory.addSRClientInstance(Map(AbrisConfig.SCHEMA_REGISTRY_URL -> SchemaRegistryURL),
      new ConfluentMockRegistryClient(MockSchemaRegistryClient))
  }

  "transform" should "decode the value-dataframe" in {
    // given
    MockSchemaRegistryClient.register(s"$Topic-value", ValueSchemaAvro)
    val records = createValueRecords(1, 100)
    val serializer = new KafkaAvroSerializer(MockSchemaRegistryClient)
    val rows = records.map(record => Row(serializer.serialize(Topic, record)))
    val schema = new StructType().add("value", BinaryType)
    val memoryStream = new MemoryStream[Row](1, spark.sqlContext)(RowEncoder(schema))
    memoryStream.addData(rows)
    val df = memoryStream.toDF()
    val queryName = "dummyQuery"
    val decoder = createBasicDecoder()

    // when
    val transformedDf = decoder.transform(df)
    executeQuery(transformedDf, queryName)

    // then
    val outputDf = spark.sql(s"select * from $queryName")
    outputDf.count() shouldBe records.size
    val result = outputDf.select("col1", "col2").collect()
    result.map(_.getAs[Int]("col1")) should contain theSameElementsAs records.map(_.get("col1"))
    result.map(_.getAs[String]("col2")) should contain theSameElementsAs records.map(_.get("col2"))
  }

  it should "decode the key-value dataframe" in {
    // given
    MockSchemaRegistryClient.register(s"$Topic-key", KeySchemaAvro)
    MockSchemaRegistryClient.register(s"$Topic-value", ValueSchemaAvro)
    val decoder = createBasicDecoder(Map(KEY_CONSUME_KEYS -> "true"))

    val schemaCatalyst = new StructType()
      .add("key", BinaryType)
      .add("value", BinaryType)
    val keySerializer = new KafkaAvroSerializer(MockSchemaRegistryClient)
    import scala.collection.JavaConverters._
    keySerializer.configure(Map("schema.registry.url" -> "").asJava, true)
    val serializer = new KafkaAvroSerializer(MockSchemaRegistryClient)
    val valueRecords = createValueRecords(1, 50)
    val keyRecords = createKeyRecords(1, 50)
    val rows = keyRecords.zip(valueRecords).map(record => Row(keySerializer.serialize(Topic, record._1), serializer.serialize(Topic, record._2)))
    val memoryStream = new MemoryStream[Row](1, spark.sqlContext)(RowEncoder(schemaCatalyst))
    memoryStream.addData(rows)
    val df = memoryStream.toDF()
    val queryName = "dummyQuery"

    // when
    val transformedDf = decoder.transform(df)
    executeQuery(transformedDf, queryName)

    // then
    val outputDf = spark.sql(s"select * from $queryName")
    outputDf.count() shouldBe valueRecords.size
    val result = outputDf.select("key__col1", "col1", "col2").collect()
    result.map(_.getAs[Long]("key__col1")) should contain theSameElementsAs keyRecords.map(_.get("col1"))
    result.map(_.getAs[Int]("col1")) should contain theSameElementsAs valueRecords.map(_.get("col1"))
    result.map(_.getAs[String]("col2")) should contain theSameElementsAs valueRecords.map(_.get("col2"))
  }

  it should "decode the value dataframe, while keeping columns" in {
    // given
    MockSchemaRegistryClient.register(s"$Topic-value", ValueSchemaAvro)
    val decoder = createBasicDecoder(Map(
      KEY_KEEP_COLUMNS -> "offset, partition"
    ))

    val schemaCatalyst = new StructType()
      .add("value", BinaryType)
      .add("offset", LongType)
      .add("partition", IntegerType)
    val serializer = new KafkaAvroSerializer(MockSchemaRegistryClient)
    val valueRecords = createValueRecords(1, 100)
    val offsets = 101L to 200L
    val partitions = (1 to 100).map(_ % 5)

    val rows = (valueRecords, offsets, partitions).zipped.toList.map(
      record => Row(serializer.serialize(Topic, record._1), record._2, record._3))
    val memoryStream = new MemoryStream[Row](1, spark.sqlContext)(RowEncoder(schemaCatalyst))
    memoryStream.addData(rows)
    val df = memoryStream.toDF()
    val queryName = "dummyQuery"

    // when
    val transformedDf = decoder.transform(df)
    executeQuery(transformedDf, queryName)

    // then
    val outputDf = spark.sql(s"select * from $queryName")
    outputDf.count() shouldBe valueRecords.size
    val result = outputDf.select("*").collect()
    result.map(_.getAs[Int]("col1")) should contain theSameElementsAs valueRecords.map(_.get("col1"))
    result.map(_.getAs[String]("col2")) should contain theSameElementsAs valueRecords.map(_.get("col2"))
    result.map(_.getAs[Long]("offset")) should contain theSameElementsAs offsets
    result.map(_.getAs[Int]("partition")) should contain theSameElementsAs partitions
  }

  it should "decode the key-value dataframe, while keeping columns" in {
    // given
    MockSchemaRegistryClient.register(s"$Topic-key", KeySchemaAvro)
    MockSchemaRegistryClient.register(s"$Topic-value", ValueSchemaAvro)
    val decoder = createBasicDecoder(Map(
      KEY_CONSUME_KEYS -> "true",
      KEY_KEEP_COLUMNS -> "offset, partition"
    ))

    val schemaCatalyst = new StructType()
      .add("key", BinaryType)
      .add("value", BinaryType)
      .add("offset", LongType)
      .add("partition", IntegerType)
    val keySerializer = new KafkaAvroSerializer(MockSchemaRegistryClient)
    import scala.collection.JavaConverters._
    keySerializer.configure(Map("schema.registry.url" -> "").asJava, true)
    val serializer = new KafkaAvroSerializer(MockSchemaRegistryClient)
    val valueRecords = createValueRecords(1, 100)
    val keyRecords = createKeyRecords(1, 100)
    val offsets = 101L to 200L
    val partitions = (1 to 100).map(_ % 5)

    val rows = zip4(keyRecords, valueRecords, offsets, partitions).map(
      record => Row(keySerializer.serialize(Topic, record._1), serializer.serialize(Topic, record._2), record._3, record._4))
    val memoryStream = new MemoryStream[Row](1, spark.sqlContext)(RowEncoder(schemaCatalyst))
    memoryStream.addData(rows)
    val df = memoryStream.toDF()
    val queryName = "dummyQuery"

    // when
    val transformedDf = decoder.transform(df)
    executeQuery(transformedDf, queryName)

    // then
    val outputDf = spark.sql(s"select * from $queryName")
    outputDf.count() shouldBe valueRecords.size
    val result = outputDf.select("*").collect()
    result.map(_.getAs[Long]("key__col1")) should contain theSameElementsAs keyRecords.map(_.get("col1"))
    result.map(_.getAs[Int]("col1")) should contain theSameElementsAs valueRecords.map(_.get("col1"))
    result.map(_.getAs[String]("col2")) should contain theSameElementsAs valueRecords.map(_.get("col2"))
    result.map(_.getAs[Long]("offset")) should contain theSameElementsAs offsets
    result.map(_.getAs[Int]("partition")) should contain theSameElementsAs partitions
  }

  it should "make value columns nullable if disableNullabilityPreservation is true" in {
    // given
    MockSchemaRegistryClient.register(s"$Topic-value", ValueSchemaAvro)
    val records = createValueRecords(1, 100)
    val serializer = new KafkaAvroSerializer(MockSchemaRegistryClient)
    val rows = records.map(record => Row(serializer.serialize(Topic, record)))
    val schema = new StructType().add("value", BinaryType)
    val memoryStream = new MemoryStream[Row](1, spark.sqlContext)(RowEncoder(schema))
    memoryStream.addData(rows)
    val df = memoryStream.toDF()
    val decoder = createBasicDecoder(Map(
      KEY_DISABLE_NULLABILITY_PRESERVATION -> "true"
    ))

    // when
    val resultDf = decoder.transform(df)

    // then
    val fields = resultDf.schema.toList
    fields.foreach(_.nullable shouldBe true)
  }


  it should "throw an exception if there is a column name collision between columns to keep and value columns" in {
    // given
    MockSchemaRegistryClient.register(s"$Topic-value", ValueSchemaAvro)
    val decoder = createBasicDecoder(Map(
      KEY_KEEP_COLUMNS -> "col1, col2"
    ))

    val schemaCatalyst = new StructType()
      .add("value", BinaryType)
      .add("col1", LongType)
      .add("col2", IntegerType)
    val memoryStream = new MemoryStream[Row](1, spark.sqlContext)(RowEncoder(schemaCatalyst))
    val df = memoryStream.toDF()

    // when
    val result = the[Exception] thrownBy decoder.transform(df)
    result.getMessage should include("col1, col2")
  }


  it should "throw an exception if there is a column name collision between columns to keep, key and value columns" in {
    // given
    MockSchemaRegistryClient.register(s"$Topic-key", KeySchemaAvro)
    MockSchemaRegistryClient.register(s"$Topic-value", ValueSchemaAvro)
    val decoder = createBasicDecoder(Map(
      KEY_CONSUME_KEYS -> "true",
      KEY_KEEP_COLUMNS -> "key__col1, col1"
    ))

    val schemaCatalyst = new StructType()
      .add("key", BinaryType)
      .add("value", BinaryType)
      .add("key__col1", LongType)
      .add("col1", IntegerType)

    val memoryStream = new MemoryStream[Row](1, spark.sqlContext)(RowEncoder(schemaCatalyst))
    val df = memoryStream.toDF()

    // when
    val result = the[Exception] thrownBy decoder.transform(df)

    // then
    result.getMessage should include("key__col1, col1")
  }

  "apply" should "throw if topic is not configured" in {
    val config = new BaseConfiguration
    config.addProperty(KEY_SCHEMA_REGISTRY_VALUE_SCHEMA_ID, SchemaRegistryValueSchemaId)
    config.addProperty(KEY_SCHEMA_REGISTRY_URL, SchemaRegistryURL)

    val throwable = intercept[IllegalArgumentException](ConfluentAvroDecodingTransformer(config))
    assert(throwable.getMessage.toLowerCase.contains("topic"))
  }

  it should "create avro stream decoder instance with schema registry settings for value schema" in {
    MockSchemaRegistryClient.register(s"$Topic-value", DummySchema)

    val decoder = createBasicDecoder()

    getSchemaRegistryConf(decoder.valueAvroConfig).get(AbrisConfig.SCHEMA_REGISTRY_URL) shouldBe SchemaRegistryURL
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
    config.addProperty(KEY_USE_ADVANCED_SCHEMA_CONVERSION, "true")

    val decoder = ConfluentAvroDecodingTransformer(config).asInstanceOf[ConfluentAvroDecodingTransformer]

    getSchemaRegistryConf(decoder.valueAvroConfig).get(AbrisConfig.SCHEMA_REGISTRY_URL) shouldBe SchemaRegistryURL
    getSchemaRegistryConf(decoder.keyAvroConfigOpt.get).get(AbrisConfig.SCHEMA_REGISTRY_URL) shouldBe SchemaRegistryURL
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

  private def createBasicDecoder(additionalConfig: Map[String, String] = Map()) = {
    val config = new DynamicCombinedConfiguration()
    config.setListDelimiterHandler(new DefaultListDelimiterHandler(','))
    config.addProperty(KEY_TOPIC, Topic)
    config.addProperty(KEY_SCHEMA_REGISTRY_URL, SchemaRegistryURL)
    config.addProperty(KEY_SCHEMA_REGISTRY_VALUE_SCHEMA_ID, SchemaRegistryValueSchemaId)
    config.addProperty(KEY_SCHEMA_REGISTRY_VALUE_NAMING_STRATEGY, AbrisConfigUtil.TopicNameStrategy)
    additionalConfig.foreach(entry => config.addProperty(entry._1, entry._2))

    ConfluentAvroDecodingTransformer(config).asInstanceOf[ConfluentAvroDecodingTransformer]
  }

  private def createValueRecords(lowerLimit: Int, upperLimit: Int) = {
    (lowerLimit to upperLimit).map(i => {
      val record = new GenericData.Record(ValueSchemaAvro)
      record.put("col1", i)
      record.put("col2", s"hello_$i")
      record
    })
  }

  private def createKeyRecords(lowerLimit: Int, upperLimit: Int) = {
    (lowerLimit to upperLimit).map(i => {
      val record = new GenericData.Record(KeySchemaAvro)
      record.put("col1", (upperLimit - i).toLong)
      record
    })
  }

  private def executeQuery(df: DataFrame, queryName: String) = {
    val query = df
      .writeStream
      .trigger(Trigger.Once)
      .queryName(queryName)
      .format("memory")
      .start()
    query.awaitTermination()
  }

  private def zip4[A, B, C, D](as: Seq[A], bs: Seq[B], cs: Seq[C], ds: Seq[D]) = {
    as zip bs zip cs zip ds map {
      case (((a, b), c), d) => (a, b, c, d)
    }
  }
}
