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
import java.util
import java.util.UUID.randomUUID
import java.util.{Collections, Properties}

import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.util.Utf8
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import za.co.absa.abris.avro.read.confluent.SchemaManagerFactory
import za.co.absa.commons.io.TempDirectory
import za.co.absa.commons.spark.SparkTestBase
import za.co.absa.abris.avro.registry.SchemaSubject

/**
 * This e2e test requires a Docker installation on the executing machine.
 */
class KafkaToKafkaDeduplicationDockerTest extends FlatSpec with Matchers with SparkTestBase with BeforeAndAfter {

  private val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  private val baseDir = TempDirectory("hyperdriveE2eTest").deleteOnExit()
  private val baseDirPath = baseDir.path.toUri
//  private val checkpointDir = s"$baseDirPath/checkpoint"
  private val checkpointDir = s"/tmp/bla1/checkpoint"

  behavior of "CommandLineIngestionDriver"

  private val hyperdriveIdSchemaString =
    raw"""{"type": "record", "name": "hyperdrive_id_record", "fields": [
         |{"type": "string", "name": "source_offset", "nullable": true},
         |{"type": "long", "name": "source_partition", "nullable": true}
         |]}
         |""".stripMargin

  private def schemaV1String(name: String) =
    raw"""{"type": "record", "name": "$name", "fields": [
         |{"type": "int", "name": "record_id"},
         |{"type": "string", "name": "value_field", "nullable": false}
         |]}""".stripMargin

  private def schemaV2String(name: String) =
    raw"""{"type": "record", "name": "$name", "fields": [
         |{"type": "int", "name": "record_id"},
         |{"type": ["null", "string"], "name": "value_field", "nullable": true}
         |]}""".stripMargin

  private def sendData(producer: KafkaProducer[GenericRecord, GenericRecord], records: Seq[GenericRecord], topic: String, partitions: Int): Unit = {
    records.zipWithIndex.foreach {
      case (record, i) =>
        val partition = i % partitions
        val producerRecord = new ProducerRecord[GenericRecord, GenericRecord](topic, partition, null, record)
        producer.send(producerRecord)
    }
  }

  private def createTopic(kafkaSchemaRegistryWrapper: KafkaSchemaRegistryWrapper, topicName: String, partitions: Int): Unit = {
    val config = new Properties()
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaSchemaRegistryWrapper.kafka.getBootstrapServers)
    val localKafkaAdmin = AdminClient.create(config)
    val replication = 1.toShort
    val topic = new NewTopic(topicName, partitions, replication)
    val topicCreationFut = localKafkaAdmin.createTopics(util.Arrays.asList(topic)).all()
    while(!topicCreationFut.isDone) {}
  }

  it should "execute the whole kafka-to-kafka pipeline" in {
    // given
    val kafkaSchemaRegistryWrapper = new KafkaSchemaRegistryWrapper
    val sourceTopic = "deduplication_src"
    val destinationTopic = "deduplication_dest"
//    val sourceTopicPartitions = 5
    val sourceTopicPartitions = 1
//    val destinationTopicPartitions = 3
    val destinationTopicPartitions = 1
    val schemaManager = SchemaManagerFactory.create(Map("schema.registry.url" -> kafkaSchemaRegistryWrapper.schemaRegistryUrl))
    val subject = SchemaSubject.usingTopicNameStrategy(sourceTopic)
    val parserV1 = new Parser()
    val schemaV1 = parserV1.parse(schemaV1String(sourceTopic))
    val parserV2 = new Parser()
    val schemaV2 = parserV2.parse(schemaV2String(sourceTopic))
    val schemaV1Id = schemaManager.register(subject, schemaV1)
    val schemaV2Id = schemaManager.register(subject, schemaV2)

    val producer = createProducer(kafkaSchemaRegistryWrapper)
    createTopic(kafkaSchemaRegistryWrapper, sourceTopic, sourceTopicPartitions)
    createTopic(kafkaSchemaRegistryWrapper, destinationTopic, destinationTopicPartitions)

    val recordsV1 = (0 until 50).map(i => {
      val valueRecord = new GenericData.Record(schemaV1)
      valueRecord.put("record_id", i)
      valueRecord.put("value_field", s"valueHello_$i")
      valueRecord
    })
    val recordsV2 = (50 until 100).map(i => {
      val valueRecord = new GenericData.Record(schemaV2)
      valueRecord.put("record_id", i)
      valueRecord.put("value_field", null)
      valueRecord
    })
    sendData(producer, recordsV1, sourceTopic, sourceTopicPartitions)
    sendData(producer, recordsV2, sourceTopic, sourceTopicPartitions)

    Thread.sleep(3000)

    val driverConfig = Map(
      // Pipeline settings
      "component.ingestor" -> "spark",
      "component.reader" -> "za.co.absa.hyperdrive.ingestor.implementation.reader.kafka.KafkaStreamReader",
      "component.transformer.id.0" -> "[column.copy]",
      "component.transformer.class.[column.copy]" -> "za.co.absa.hyperdrive.ingestor.implementation.transformer.column.copy.ColumnCopyStreamTransformer",
      "component.transformer.id.1" -> "[avro.decoder]",
      "component.transformer.class.[avro.decoder]" -> "za.co.absa.hyperdrive.ingestor.implementation.transformer.avro.confluent.ConfluentAvroDecodingTransformer",
      "component.transformer.id.2" -> "[kafka.deduplicator]",
      "component.transformer.class.[kafka.deduplicator]" -> "za.co.absa.hyperdrive.ingestor.implementation.transformer.deduplicate.kafka.DeduplicateKafkaSinkTransformer",
      "component.transformer.id.3" -> "[avro.encoder]",
      "component.transformer.class.[avro.encoder]" -> "za.co.absa.hyperdrive.ingestor.implementation.transformer.avro.confluent.ConfluentAvroEncodingTransformer",
      "component.writer" -> "za.co.absa.hyperdrive.ingestor.implementation.writer.kafka.KafkaStreamWriter",

      // Spark settings
      "ingestor.spark.app.name" -> "ingestor-app",
      "ingestor.spark.termination.timeout" -> "60000",

      // Source(Kafka) settings
      "reader.kafka.topic" -> sourceTopic,
      "reader.kafka.brokers" -> kafkaSchemaRegistryWrapper.kafkaUrl,
      "reader.option.maxOffsetsPerTrigger" -> "20",

      "transformer.[column.copy].columns.copy.from" -> "offset, partition",
      "transformer.[column.copy].columns.copy.to" -> "hyperdrive_id.source_offset, hyperdrive_id.source_partition",

      // Avro Decoder (ABRiS) settings
      "transformer.[avro.decoder].schema.registry.url" -> kafkaSchemaRegistryWrapper.schemaRegistryUrl,
      "transformer.[avro.decoder].value.schema.id" -> s"$schemaV1Id",
      "transformer.[avro.decoder].value.schema.naming.strategy" -> "topic.name",
      "transformer.[avro.decoder].keep.columns" -> "hyperdrive_id",

      // comma separated list of columns to select
      "transformer.[kafka.deduplicator].source.id.columns" -> "offset,partition",
      "transformer.[kafka.deduplicator].destination.id.columns" -> "${transformer.[kafka.deduplicator].source.id.columns}",
      "transformer.[kafka.deduplicator].schema.registry.url" -> "${transformer.[avro.decoder].schema.registry.url}",

      // Avro Encoder (ABRiS) settings
      "transformer.[avro.encoder].schema.registry.url" -> "${transformer.[avro.decoder].schema.registry.url}",
      "transformer.[avro.encoder].value.schema.naming.strategy" -> "topic.name",

      // Sink(Kafka) settings
      "writer.common.checkpoint.location" -> (checkpointDir + "/${reader.kafka.topic}"),
      "writer.common.trigger.type" -> "ProcessingTime",
      "writer.kafka.topic" -> destinationTopic,
      "writer.kafka.brokers" -> "${reader.kafka.brokers}"
    )
    val driverConfigArray = driverConfig.map { case (key, value) => s"$key=$value" }.toArray

    // when
    var exceptionWasThrown = false
    try {
      CommandLineIngestionDriver.main(driverConfigArray)
    } catch {
      case e: Exception =>
        exceptionWasThrown = true
        val retryConfig = driverConfig ++ Map(
          "transformer.[avro.decoder].value.schema.id" -> s"$schemaV2Id",
          "writer.common.trigger.type" -> "Once",
          "reader.option.maxOffsetsPerTrigger" -> "9999"
        )
        val retryConfigArray = retryConfig.map { case (key, value) => s"$key=$value"}.toArray
        CommandLineIngestionDriver.main(retryConfigArray)
        CommandLineIngestionDriver.main(retryConfigArray)
    }

    exceptionWasThrown shouldBe true

    // then
    fs.exists(new Path(s"$checkpointDir/$sourceTopic")) shouldBe true

    val allRecords = recordsV1.size + recordsV2.size
    val consumer = createConsumer(kafkaSchemaRegistryWrapper)
    consumer.subscribe(Collections.singletonList(destinationTopic))
    import scala.collection.JavaConverters._
    val records = consumer.poll(Duration.ofMillis(1000L)).asScala.toList
//    records.size shouldBe allRecords

    val valueFieldNames = records.head.value().getSchema.getFields.asScala.map(_.name())
    valueFieldNames should contain theSameElementsAs List("record_id", "value_field", "hyperdrive_id")
    val actual = records.map(_.value().get("record_id"))
    val expected = 0 until allRecords
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
