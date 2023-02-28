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

import java.time.Duration
import java.util.UUID.randomUUID
import java.util.{Collections, Properties}
import org.apache.avro.Schema.{Parser, Type}
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.util.Utf8
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.clients.consumer.KafkaConsumer
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
class KafkaToKafkaDockerTest extends AnyFlatSpec with Matchers with SparkTestBase with BeforeAndAfter {

  private val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  private val baseDir = TempDirectory("hyperdriveE2eTest").deleteOnExit()
  private val baseDirPath = baseDir.path.toUri
  private val checkpointDir = s"$baseDirPath/checkpoint"

  behavior of "CommandLineIngestionDriver"

  it should "execute the whole kafka-to-kafka pipeline" in {
    // given
    val kafkaSchemaRegistryWrapper = new KafkaSchemaRegistryWrapper

    val sourceTopic = "e2etestsrc"
    val destinationTopic = "e2etestdest"
    val numberOfRecords = 50

    val keySchemaString = raw"""{"type": "record", "name": "$sourceTopic", "fields": [
      {"type": "int", "name": "some_id"},
      {"type": "string", "name": "key_field"}
      ]}"""
    val keySchema = new Parser().parse(keySchemaString)

    val valueSchemaString = raw"""{"type": "record", "name": "$sourceTopic", "fields": [
      {"type": "int", "name": "some_id"},
      {"type": ["string", "null"], "name": "value_field"}
      ]}"""
    val valueSchema = new Parser().parse(valueSchemaString)

    val producer = createProducer(kafkaSchemaRegistryWrapper)
    for (i <- 0 until numberOfRecords) {
      val keyRecord = new GenericData.Record(keySchema)
      keyRecord.put("some_id", i / 5)
      keyRecord.put("key_field", "keyHello")

      val valueRecord = new GenericData.Record(valueSchema)
      valueRecord.put("some_id", i)
      valueRecord.put("value_field", "valueHello")

      val producerRecord = new ProducerRecord[GenericRecord, GenericRecord](sourceTopic, keyRecord, valueRecord)
      producer.send(producerRecord)
    }
    Thread.sleep(3000)

    val driverConfig = Map(
      // Pipeline settings
      "component.ingestor" -> "spark",
      "component.reader" -> "za.co.absa.hyperdrive.ingestor.implementation.reader.kafka.KafkaStreamReader",
      "component.transformer.id.0" -> "[avro.decoder]",
      "component.transformer.class.[avro.decoder]" -> "za.co.absa.hyperdrive.ingestor.implementation.transformer.avro.confluent.ConfluentAvroDecodingTransformer",
      "component.transformer.id.1" -> "column.selector",
      "component.transformer.class.column.selector" -> "za.co.absa.hyperdrive.ingestor.implementation.transformer.column.selection.ColumnSelectorStreamTransformer",
      "component.transformer.id.2" -> "[column.copy]",
      "component.transformer.class.[column.copy]" -> "za.co.absa.hyperdrive.ingestor.implementation.transformer.column.copy.ColumnCopyStreamTransformer",
      "component.transformer.id.3" -> "[avro.encoder]",
      "component.transformer.class.[avro.encoder]" -> "za.co.absa.hyperdrive.ingestor.implementation.transformer.avro.confluent.ConfluentAvroEncodingTransformer",
      "component.writer" -> "za.co.absa.hyperdrive.ingestor.implementation.writer.kafka.KafkaStreamWriter",

      // Spark settings
      "ingestor.spark.termination.method" -> "ProcessAllAvailable",

      // Source(Kafka) settings
      "reader.kafka.topic" -> sourceTopic,
      "reader.kafka.brokers" -> kafkaSchemaRegistryWrapper.kafkaUrl,

      // Avro Decoder (ABRiS) settings
      "transformer.[avro.decoder].schema.registry.url" -> kafkaSchemaRegistryWrapper.schemaRegistryUrl,
      "transformer.[avro.decoder].value.schema.id" -> "latest",
      "transformer.[avro.decoder].value.schema.naming.strategy" -> "topic.name",
      "transformer.[avro.decoder].consume.keys" -> "true",
      "transformer.[avro.decoder].key.schema.id" -> "latest",
      "transformer.[avro.decoder].key.schema.naming.strategy" -> "topic.name",

      // comma separated list of columns to select
      "transformer.column.selector.columns.to.select" -> "*",

      // copy transformer settings
      "transformer.[column.copy].columns.copy.from" -> "some_id, value_field",
      "transformer.[column.copy].columns.copy.to" -> "grouped.some_id, grouped.value_field",

      // Avro Encoder (ABRiS) settings
      "transformer.[avro.encoder].schema.registry.url" -> "${transformer.[avro.decoder].schema.registry.url}",
      "transformer.[avro.encoder].value.schema.naming.strategy" -> "topic.name",
      "transformer.[avro.encoder].produce.keys" -> "true",
      "transformer.[avro.encoder].key.schema.naming.strategy" -> "topic.name",

      // Sink(Kafka) settings
      "writer.common.checkpoint.location" -> (checkpointDir + "/${reader.kafka.topic}"),
      "writer.common.trigger.type" -> "ProcessingTime",
      "writer.common.trigger.processing.time" -> "1000",
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
    records.size shouldBe numberOfRecords

    val keyFieldNames = records.head.key().getSchema.getFields.asScala.map(_.name())
    keyFieldNames should contain theSameElementsAs List("some_id", "key_field")
    records.map(_.key().get("some_id")) should contain theSameElementsInOrderAs List.tabulate(numberOfRecords)(_ / 5)
    records.map(_.key().get("key_field")).distinct should contain theSameElementsAs List(new Utf8("keyHello"))

    records.head.value().getSchema.getField("some_id").schema().getType shouldBe Type.INT
    records.head.value().getSchema.getField("value_field").schema().getTypes
      .asScala.map(_.getType) should contain theSameElementsAs Seq(Type.STRING, Type.NULL)

    val valueFieldNames = records.head.value().getSchema.getFields.asScala.map(_.name())
    valueFieldNames should contain theSameElementsAs List("some_id", "value_field", "grouped")
    val allIds = records.map(_.value().get("some_id"))
    val allValues = records.map(_.value().get("value_field"))
    allIds should contain theSameElementsInOrderAs List.range(0, numberOfRecords)
    allValues.distinct should contain theSameElementsAs List(new Utf8("valueHello"))
    val idsInGrouped = records.map(_.value().get("grouped").asInstanceOf[GenericRecord].get("some_id"))
    val valuesInGrouped = records.map(_.value().get("grouped").asInstanceOf[GenericRecord].get("value_field"))
    idsInGrouped shouldBe allIds
    valuesInGrouped shouldBe allValues
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
