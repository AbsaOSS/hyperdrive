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
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import za.co.absa.abris.avro.read.confluent.SchemaManagerFactory
import za.co.absa.abris.config.AbrisConfig
import za.co.absa.commons.spark.SparkTestBase
import za.co.absa.hyperdrive.ingestor.implementation.testutils.HyperdriveMockSchemaRegistryClient
import za.co.absa.hyperdrive.ingestor.implementation.transformer.avro.confluent.ConfluentAvroEncodingTransformer._
import za.co.absa.hyperdrive.ingestor.implementation.utils.AbrisConfigUtil
import za.co.absa.hyperdrive.ingestor.implementation.writer.kafka.KafkaStreamWriter

class TestConfluentAvroEncodingTransformer extends FlatSpec with Matchers with BeforeAndAfter with SparkTestBase {

  private val topic = "topic"
  private val SchemaRegistryURL = "http://localhost:8081"

  behavior of ConfluentAvroEncodingTransformer.getClass.getSimpleName

  before {
    val mockSchemaRegistryClient = new HyperdriveMockSchemaRegistryClient()
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

  it should "encode the values" in {
    // given
    import spark.implicits._
    val queryName = "dummyQuery"
    val input = MemoryStream[Int](1, spark.sqlContext)
    input.addData(1 to 100)
    val df = input.toDF()

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
}
