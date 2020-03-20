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

import java.nio.file.Files
import java.time.Duration
import java.util.UUID.randomUUID
import java.util.{Collections, Properties}

import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.util.Utf8
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.logging.log4j.LogManager
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import za.co.absa.abris.avro.read.confluent.SchemaManager
import za.co.absa.hyperdrive.testutils.SparkTestBase


/**
 * This e2e test requires a Docker installation on the executing machine.
 */
class KafkaToKafkaDockerTest extends FlatSpec with Matchers with SparkTestBase with BeforeAndAfter {

  private val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  private val baseDirPath = Files.createTempDirectory("hyperdriveE2eTest")
  private val baseDir = baseDirPath.toUri
  private val checkpointDir = s"$baseDir/checkpoint"

  behavior of "CommandLineIngestionDriver"

  it should "execute the whole kafka-to-kafka pipeline" in {
    // given
    val kafkaSchemaRegistryWrapper = new KafkaSchemaRegistryWrapper

    val sourceTopic = "e2etestsrc"
    val destinationTopic = "e2etestdest"
    val numberOfRecords = 50
    val schemaString = raw"""{"type": "record", "name": "$sourceTopic", "fields": [
      {"type": "string", "name": "field1"},
      {"type": "int", "name": "field2"}
      ]}"""
    val schema = new Parser().parse(schemaString)

    val producer = createProducer(kafkaSchemaRegistryWrapper)
    for (i <- 0 until numberOfRecords) {
      val record = new GenericData.Record(schema)
      record.put("field1", "hello")
      record.put("field2", i)
      val producerRecord = new ProducerRecord[Int, GenericRecord](sourceTopic, 1, record)
      producer.send(producerRecord)
    }
    Thread.sleep(3000)

    val driverConfig = Map(
      // Pipeline settings
      "component.ingestor" -> "spark",
      "component.reader" -> "za.co.absa.hyperdrive.ingestor.implementation.reader.kafka.KafkaStreamReader",
      "component.decoder" -> "za.co.absa.hyperdrive.ingestor.implementation.decoder.avro.confluent.ConfluentAvroKafkaStreamDecoder",
      "component.manager" -> "za.co.absa.hyperdrive.ingestor.implementation.manager.checkpoint.CheckpointOffsetManager",
      "component.transformer" -> "za.co.absa.hyperdrive.ingestor.implementation.transformer.column.selection.ColumnSelectorStreamTransformer",
      "component.writer" -> "za.co.absa.hyperdrive.ingestor.implementation.writer.kafka.KafkaStreamWriter",

      // Spark settings
      "ingestor.spark.app.name" -> "ingestor-app",

      // Source(Kafka) settings
      "reader.kafka.topic" -> sourceTopic,
      "reader.kafka.brokers" -> kafkaSchemaRegistryWrapper.kafkaUrl,

      // Offset management(checkpointing) settings
      "manager.checkpoint.base.location" -> (checkpointDir + "/${reader.kafka.topic}"),

      // Format(ABRiS) settings
      "decoder.avro.schema.registry.url" -> kafkaSchemaRegistryWrapper.schemaRegistryUrl,
      "decoder.avro.value.schema.id" -> "latest",
      "decoder.avro.value.schema.naming.strategy" -> "topic.name",

      // Transformations(Enceladus) settings
      // comma separated list of columns to select
      "transformer.columns.to.select" -> "*",

      // Sink(Kafka) settings
      "writer.kafka.topic" -> destinationTopic,
      "writer.kafka.brokers" -> "${reader.kafka.brokers}",
      "writer.kafka.schema.registry.url" -> "${decoder.avro.schema.registry.url}",
      "writer.kafka.value.schema.naming.strategy" -> "topic.name"
    )
    val driverConfigArray = driverConfig.map { case (key, value) => s"$key=$value" }.toArray

    // when
    CommandLineIngestionDriver.main(driverConfigArray)

    // then
    fs.exists(new Path(s"$checkpointDir/$sourceTopic")) shouldBe true

    val consumer = createConsumer(kafkaSchemaRegistryWrapper)
    consumer.subscribe(Collections.singletonList(destinationTopic))
    import scala.collection.JavaConverters._
    val records = consumer.poll(Duration.ofMillis(500L)).asScala.toList
    records.size shouldBe numberOfRecords
    val fieldNames = records.head.value().getSchema.getFields.asScala.map(_.name())
    fieldNames should contain theSameElementsAs List("field1", "field2")
    records.map(_.value().get("field1")).distinct should contain theSameElementsAs List(new Utf8("hello"))
    records.map(_.value().get("field2")) should contain theSameElementsAs List.range(0, numberOfRecords)
  }

  after {
    SchemaManager.reset()
    fs.delete(new Path(baseDir), true)
  }

  def createProducer(kafkaSchemaRegistryWrapper: KafkaSchemaRegistryWrapper): KafkaProducer[Int, GenericRecord] = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaSchemaRegistryWrapper.kafka.getBootstrapServers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer")
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaToKafkaProducer")
    props.put(ProducerConfig.ACKS_CONFIG, "1")
    kafkaSchemaRegistryWrapper.createProducer(props)
  }

  def createConsumer(kafkaSchemaRegistryWrapper: KafkaSchemaRegistryWrapper): KafkaConsumer[Int, GenericRecord] = {
    import org.apache.kafka.clients.consumer.ConsumerConfig
    val props = new Properties()
    props.put(ConsumerConfig.GROUP_ID_CONFIG, randomUUID.toString)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaSchemaRegistryWrapper.kafka.getBootstrapServers)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer")
    kafkaSchemaRegistryWrapper.createConsumer(props)
  }

}
