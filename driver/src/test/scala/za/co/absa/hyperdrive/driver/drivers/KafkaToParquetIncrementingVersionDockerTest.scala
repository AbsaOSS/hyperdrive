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

package za.co.absa.hyperdrive.driver.drivers

import java.util.Properties

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import za.co.absa.abris.avro.read.confluent.SchemaManager
import za.co.absa.commons.io.TempDirectory
import za.co.absa.commons.spark.SparkTestBase

/**
 * This e2e test requires a Docker installation on the executing machine.
 */
class KafkaToParquetIncrementingVersionDockerTest extends FlatSpec with Matchers with SparkTestBase with BeforeAndAfter {

  private val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  private val baseDir = TempDirectory("hyperdriveE2eTest").deleteOnExit()
  private val baseDirPath = baseDir.path.toUri
  private val checkpointDir = s"$baseDirPath/checkpoint"
  private val destinationDir = s"$baseDirPath/destination"

  behavior of "CommandLineIngestionDriver"

  before {
    fs.mkdirs(new Path(destinationDir))
  }

  it should "execute the whole kafka-to-parquet pipeline with the incrementing version transformer" in {
    // given
    val kafkaSchemaRegistryWrapper = new KafkaSchemaRegistryWrapper
    val topic = "e2etest"
    val ingestionSize = 50
    val schemaString = raw"""{"type": "record", "name": "$topic", "fields": [
      {"type": "string", "name": "field1"},
      {"type": "int", "name": "field2"}
      ]}"""
    val schema = new Parser().parse(schemaString)
    val producer = createProducer(kafkaSchemaRegistryWrapper)

    val driverConfig = Map(
      // Pipeline settings
      "component.ingestor" -> "spark",
      "component.reader" -> "za.co.absa.hyperdrive.ingestor.implementation.reader.kafka.KafkaStreamReader",
      "component.decoder" -> "za.co.absa.hyperdrive.ingestor.implementation.decoder.avro.confluent.ConfluentAvroKafkaStreamDecoder",
      "component.transformer.id.1" -> "[version.incrementer]",
      "component.transformer.class.[version.incrementer]" -> "za.co.absa.hyperdrive.ingestor.implementation.transformer.dateversion.AddDateVersionTransformer",
      "component.writer" -> "za.co.absa.hyperdrive.ingestor.implementation.writer.parquet.ParquetStreamWriter",

      // Spark settings
      "ingestor.spark.app.name" -> "ingestor-app",
      "ingestor.spark.termination.method" -> "AwaitTermination",

      // Source(Kafka) settings
      "reader.kafka.topic" -> topic,
      "reader.kafka.brokers" -> kafkaSchemaRegistryWrapper.kafkaUrl,

      // Format(ABRiS) settings
      "decoder.avro.schema.registry.url" -> kafkaSchemaRegistryWrapper.schemaRegistryUrl,
      "decoder.avro.value.schema.id" -> "latest",
      "decoder.avro.value.schema.naming.strategy" -> "topic.name",

      // Transformations(Enceladus) settings
      // comma separated list of columns to select
      "transformer.[version.incrementer].report.date" -> "2020-03-31",
      "transformer.[version.incrementer].destination.directory" -> "${writer.parquet.destination.directory}",

      // Sink(Parquet) settings
      "writer.common.checkpoint.location" -> (checkpointDir + "/${reader.kafka.topic}"),
      "writer.parquet.destination.directory" -> destinationDir,
      "writer.parquet.partition.columns" -> "hyperdrive_date, hyperdrive_version",
      "writer.parquet.metadata.check" -> "true"
    )
    val driverConfigArray = driverConfig.map { case (key, value) => s"$key=$value" }.toArray

    // when (1)
    produceMessage(ingestionSize, producer, schema, topic)
    CommandLineIngestionDriver.main(driverConfigArray)

    // then (1)
    fs.exists(new Path(s"$checkpointDir/$topic")) shouldBe true

    fs.exists(new Path(s"$destinationDir/hyperdrive_date=2020-03-31/hyperdrive_version=1")) shouldBe true
    fs.exists(new Path(s"$destinationDir/hyperdrive_date=2020-03-31/hyperdrive_version=2")) shouldBe false
    val df = spark.read.parquet(destinationDir)
    df.count shouldBe ingestionSize
    import spark.implicits._
    df.columns should contain theSameElementsAs List("field1", "field2", "hyperdrive_date", "hyperdrive_version")
    df.select("hyperdrive_version").distinct().as[Int].collect() should contain theSameElementsAs List(1)

    // when (2)
    produceMessage(ingestionSize, producer, schema, topic)
    CommandLineIngestionDriver.main(driverConfigArray)

    // then (2)
    fs.exists(new Path(s"$destinationDir/hyperdrive_date=2020-03-31/hyperdrive_version=1")) shouldBe true
    fs.exists(new Path(s"$destinationDir/hyperdrive_date=2020-03-31/hyperdrive_version=2")) shouldBe true
    val df2 = spark.read.parquet(destinationDir)
    df2.count shouldBe 2 * ingestionSize
    import spark.implicits._
    df2.columns should contain theSameElementsAs List("field1", "field2", "hyperdrive_date", "hyperdrive_version")
    df2.select("hyperdrive_version").distinct().as[Int].collect() should contain theSameElementsAs List(1, 2)
    df2.select("field1").distinct().as[String].collect() should contain theSameElementsAs List("hello")
    df2.select("field2").as[Int].collect() should contain theSameElementsAs (0 until ingestionSize) ++ (0 until ingestionSize)
    df2.select("hyperdrive_date").distinct()
      .as[java.sql.Date].collect() should contain theSameElementsAs List(java.sql.Date.valueOf("2020-03-31"))
  }

  after {
    SchemaManager.reset()
  }

  private def produceMessage(numberOfRecords: Int, producer: KafkaProducer[Int, GenericRecord], schema: Schema, topic: String) = {
    for (i <- 0 until numberOfRecords) {
      val record = new GenericData.Record(schema)
      record.put("field1", "hello")
      record.put("field2", i)
      val producerRecord = new ProducerRecord[Int, GenericRecord](topic, 1, record)
      producer.send(producerRecord)
    }
  }

  private def createProducer(kafkaSchemaRegistryWrapper: KafkaSchemaRegistryWrapper): KafkaProducer[Int, GenericRecord] = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaSchemaRegistryWrapper.kafka.getBootstrapServers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer")
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "AvroProducer")
    kafkaSchemaRegistryWrapper.createProducer(props)
  }

}
