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
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.abris.avro.read.confluent.SchemaManagerFactory
import za.co.absa.commons.io.TempDirectory
import za.co.absa.spark.commons.test.SparkTestBase

/**
 * This e2e test requires a Docker installation on the executing machine.
 */
class KafkaToParquetDockerTest extends AnyFlatSpec with Matchers with SparkTestBase with BeforeAndAfter {

  private val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  private val baseDir = TempDirectory("hyperdriveE2eTest").deleteOnExit()
  private val baseDirPath = baseDir.path.toUri
  private val checkpointDir = s"$baseDirPath/checkpoint"
  private val destinationDir = s"$baseDirPath/destination"

  behavior of "CommandLineIngestionDriver"

  before {
    fs.mkdirs(new Path(destinationDir))
  }

  it should "execute the whole kafka-to-parquet pipeline" in {
    // given
    val kafkaSchemaRegistryWrapper = new KafkaSchemaRegistryWrapper
    val topic = "e2etest"
    val numberOfRecords = 50
    val schemaString = raw"""{"type": "record", "name": "$topic", "fields": [
      {"type": "string", "name": "field1"},
      {"type": "int", "name": "field2"},
      {"type": "int", "name": "field3"},
      {"type": "int", "name": "field4"}

      ]}"""
    val schema = new Parser().parse(schemaString)

    val producer = createProducer(kafkaSchemaRegistryWrapper)
    for (i <- 0 until numberOfRecords) {
      val record = new GenericData.Record(schema)
      record.put("field1", "hello")
      record.put("field2", i)
      record.put("field3", i / 5)
      record.put("field4", 0)
      val producerRecord = new ProducerRecord[Int, GenericRecord](topic, 1, record)
      producer.send(producerRecord)
    }

    val driverConfig = Map(
      // Pipeline settings
      "component.ingestor" -> "spark",
      "component.reader" -> "za.co.absa.hyperdrive.ingestor.implementation.reader.kafka.KafkaStreamReader",
      "component.transformer.id.0" -> "[avro.decoder]",
      "component.transformer.class.[avro.decoder]" -> "za.co.absa.hyperdrive.ingestor.implementation.transformer.avro.confluent.ConfluentAvroDecodingTransformer",
      "component.transformer.id.1" -> "column.selector",
      "component.transformer.class.column.selector" -> "za.co.absa.hyperdrive.ingestor.implementation.transformer.column.selection.ColumnSelectorStreamTransformer",
      "component.writer" -> "za.co.absa.hyperdrive.ingestor.implementation.writer.parquet.ParquetStreamWriter",

      // Source(Kafka) settings
      "reader.kafka.topic" -> topic,
      "reader.kafka.brokers" -> kafkaSchemaRegistryWrapper.kafkaUrl,

      // Format(ABRiS) settings
      "transformer.[avro.decoder].schema.registry.url" -> kafkaSchemaRegistryWrapper.schemaRegistryUrl,
      "transformer.[avro.decoder].value.schema.id" -> "latest",
      "transformer.[avro.decoder].value.schema.naming.strategy" -> "topic.name",
      "transformer.[avro.decoder].consume.keys" -> "false",

      // Transformations(Enceladus) settings
      // comma separated list of columns to select
      "transformer.column.selector.columns.to.select" -> "field1, field2, field3",

      // Sink(Parquet) settings
      "writer.common.checkpoint.location" -> (checkpointDir + "/${reader.kafka.topic}"),
      "writer.parquet.destination.directory" -> destinationDir,
      "writer.parquet.partition.columns" -> "field3, field1",
      "writer.parquet.metadata.check" -> "true"
    )
    val driverConfigArray = driverConfig.map { case (key, value) => s"$key=$value" }.toArray

    // when
    CommandLineIngestionDriver.main(driverConfigArray)

    // then
    fs.exists(new Path(s"$checkpointDir/$topic")) shouldBe true

    val df = spark.read.parquet(destinationDir)
    df.count shouldBe numberOfRecords
    import spark.implicits._
    df.columns should contain theSameElementsAs List("field1", "field2", "field3")
    df.select("field1").distinct()
      .map(_ (0).asInstanceOf[String]).collect() should contain theSameElementsAs List("hello")
    df.select("field2")
      .map(_ (0).asInstanceOf[Int]).collect() should contain theSameElementsAs List.range(0, numberOfRecords)
    df.select("field3")
      .map(_ (0).asInstanceOf[Int]).collect() should contain theSameElementsAs List.tabulate(numberOfRecords)(_ / 5)

    fs.exists(new Path(s"$destinationDir/field3=0/field1=hello")) shouldBe true
  }

  after {
    SchemaManagerFactory.resetSRClientInstance()
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
