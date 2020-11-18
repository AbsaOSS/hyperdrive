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

import java.io.{BufferedWriter, File, FileWriter, OutputStreamWriter}
import java.nio.file.{Files, Paths}
import java.time.Duration
import java.util.UUID.randomUUID
import java.util.{Collections, Properties}

import org.apache.avro.Schema.{Parser, Type}
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.util.Utf8
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import za.co.absa.abris.avro.read.confluent.SchemaManagerFactory
import za.co.absa.commons.io.TempDirectory
import za.co.absa.commons.spark.SparkTestBase


/**
 * This e2e test requires a Docker installation on the executing machine.
 */
// TODO: Add testcase with multiple partitions
// TODO: Add testcase with at least one committed microbatch
class KafkaToKafkaDeduplicationDockerTest extends FlatSpec with Matchers with SparkTestBase with BeforeAndAfter {

  private val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  private val baseDir = TempDirectory("hyperdriveE2eTest").deleteOnExit()
  private val baseDirPath = baseDir.path.toUri
  private val checkpointDir = s"$baseDirPath/checkpoint"

  behavior of "CommandLineIngestionDriver"

  private val hyperdriveIdSchemaString =
    raw"""{"type": "record", "name": "hyperdrive_id_record", "fields": [
         |{"type": "long", "name": "source_offset", "nullable": true},
         |{"type": "long", "name": "source_partition", "nullable": true}
         |]}
         |""".stripMargin

  private def schemaString(topic: String) = raw"""
      {"type": "record", "name": "$topic", "fields": [
      {"type": "string", "name": "record_id"},
      {"type": ["string", "null"], "name": "value_field"},
      {"type": "hyperdrive_id_record", "name": "hyperdrive_id"}
      ]}"""

  private def sendData(producer: KafkaProducer[GenericRecord, GenericRecord], from: Int, to: Int, topic: String) = {
    val parser = new Parser()
    val hyperdriveIdSchema = parser.parse(hyperdriveIdSchemaString)
    val schema = parser.parse(schemaString(topic))
    for (i <- from until to) {
      val valueRecord = new GenericData.Record(schema)
      valueRecord.put("record_id", i.toString)
      valueRecord.put("value_field", s"valueHello_$i")
      val hyperdriveIdRecord = new GenericData.Record(hyperdriveIdSchema)
      hyperdriveIdRecord.put("source_offset", i.toLong)
      hyperdriveIdRecord.put("source_partition", 1L)
      valueRecord.put("hyperdrive_id", hyperdriveIdRecord)

      val producerRecord = new ProducerRecord[GenericRecord, GenericRecord](topic, valueRecord)
      producer.send(producerRecord)
    }
  }

  private def writeToFile(filename: String, content: String) = {
    val path = new Path(filename)
    val out = fs.create(path)
    out.writeBytes(content)
    out.flush()
    out.close()
  }

  it should "execute the whole kafka-to-kafka pipeline" in {
    // given
    val kafkaSchemaRegistryWrapper = new KafkaSchemaRegistryWrapper

    val sourceTopic = "deduplication_src"
    val destinationTopic = "deduplication_dest"
    val allRecords = 250
    val duplicatedRange1 = (0, 75)
    val duplicatedRange2 = (100, 175)
    val producer = createProducer(kafkaSchemaRegistryWrapper)
    sendData(producer, 0, allRecords, sourceTopic)
    sendData(producer, duplicatedRange1._1, duplicatedRange1._2, destinationTopic)
    sendData(producer, duplicatedRange2._1, duplicatedRange2._2, destinationTopic)

    val offset = raw"""v1
         |{"batchWatermarkMs":0,"batchTimestampMs":1605193344093,"conf":{"spark.sql.streaming.stateStore.providerClass":"org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider","spark.sql.streaming.flatMapGroupsWithState.stateFormatVersion":"2","spark.sql.streaming.multipleWatermarkPolicy":"min","spark.sql.streaming.aggregation.stateFormatVersion":"2","spark.sql.shuffle.partitions":"200"}}
         |{"$sourceTopic":{"0":250}}""".stripMargin
    val sources = raw"""0v1
         |{"$sourceTopic":{"0":0}}""".stripMargin
    val metadata = raw"""{"id":"7a9e78ae-3473-469c-a906-c78ffaf0f3c9"}"""

    fs.mkdirs(new Path(s"$checkpointDir/$sourceTopic/commits"))
    writeToFile(s"$checkpointDir/$sourceTopic/offsets/0", offset)
    writeToFile(s"$checkpointDir/$sourceTopic/sources/0/0", sources)
    writeToFile(s"$checkpointDir/$sourceTopic/metadata", metadata)

    Thread.sleep(3000)

    val driverConfig = Map(
      // Pipeline settings
      "component.ingestor" -> "spark",
      "component.reader" -> "za.co.absa.hyperdrive.ingestor.implementation.reader.kafka.KafkaStreamReader",
      "component.transformer.id.0" -> "[avro.decoder]",
      "component.transformer.class.[avro.decoder]" -> "za.co.absa.hyperdrive.ingestor.implementation.transformer.avro.confluent.ConfluentAvroDecodingTransformer",
      "component.transformer.id.1" -> "[kafka.deduplicator]",
      "component.transformer.class.[kafka.deduplicator]" -> "za.co.absa.hyperdrive.ingestor.implementation.transformer.deduplicate.kafka.DeduplicateKafkaSinkTransformer",
      "component.transformer.id.2" -> "[avro.encoder]",
      "component.transformer.class.[avro.encoder]" -> "za.co.absa.hyperdrive.ingestor.implementation.transformer.avro.confluent.ConfluentAvroEncodingTransformer",
      "component.writer" -> "za.co.absa.hyperdrive.ingestor.implementation.writer.kafka.KafkaStreamWriter",

      // Spark settings
      "ingestor.spark.app.name" -> "ingestor-app",

      // Source(Kafka) settings
      "reader.kafka.topic" -> sourceTopic,
      "reader.kafka.brokers" -> kafkaSchemaRegistryWrapper.kafkaUrl,

      // Avro Decoder (ABRiS) settings
      "transformer.[avro.decoder].schema.registry.url" -> kafkaSchemaRegistryWrapper.schemaRegistryUrl,
      "transformer.[avro.decoder].value.schema.id" -> "latest",
      "transformer.[avro.decoder].value.schema.naming.strategy" -> "topic.name",

      // comma separated list of columns to select
      "transformer.[kafka.deduplicator].id.column" -> "record_id",
      "transformer.[kafka.deduplicator].schema.registry.url" -> "${transformer.[avro.decoder].schema.registry.url}",

      // Avro Encoder (ABRiS) settings
      "transformer.[avro.encoder].schema.registry.url" -> "${transformer.[avro.decoder].schema.registry.url}",
      "transformer.[avro.encoder].value.schema.naming.strategy" -> "topic.name",

      // Sink(Kafka) settings
      "writer.common.checkpoint.location" -> (checkpointDir + "/${reader.kafka.topic}"),
      "writer.common.trigger.type" -> "Once",
      "writer.kafka.topic" -> destinationTopic,
      "writer.kafka.brokers" -> "${reader.kafka.brokers}"
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
    records.size shouldBe allRecords

    val valueFieldNames = records.head.value().getSchema.getFields.asScala.map(_.name())
    valueFieldNames should contain theSameElementsAs List("record_id", "value_field")
    val actual = records.map(_.value().get("record_id"))
    val expected = List.range(0, allRecords).map(i => new Utf8(i.toString))
    actual should contain theSameElementsAs expected
  }

  after {
    SchemaManagerFactory.resetSRClientInstance()
  }

  def createProducer(kafkaSchemaRegistryWrapper: KafkaSchemaRegistryWrapper): KafkaProducer[GenericRecord, GenericRecord] = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaSchemaRegistryWrapper.kafka.getBootstrapServers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer")
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaToKafkaProducer")
    props.put(ProducerConfig.ACKS_CONFIG, "1")
    kafkaSchemaRegistryWrapper.createProducer(props)
  }

  def createConsumer(kafkaSchemaRegistryWrapper: KafkaSchemaRegistryWrapper): KafkaConsumer[GenericRecord, GenericRecord] = {
    import org.apache.kafka.clients.consumer.ConsumerConfig
    val props = new Properties()
    props.put(ConsumerConfig.GROUP_ID_CONFIG, randomUUID.toString)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaSchemaRegistryWrapper.kafka.getBootstrapServers)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer")
    kafkaSchemaRegistryWrapper.createConsumer(props)
  }

}
