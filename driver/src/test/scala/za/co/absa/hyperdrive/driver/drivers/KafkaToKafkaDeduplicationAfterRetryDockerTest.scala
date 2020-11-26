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
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import za.co.absa.abris.avro.read.confluent.SchemaManagerFactory
import za.co.absa.commons.io.TempDirectory
import za.co.absa.commons.spark.SparkTestBase
import za.co.absa.abris.avro.registry.SchemaSubject
import za.co.absa.hyperdrive.shared.exceptions.IngestionException

/**
 * This e2e test requires a Docker installation on the executing machine.
 * In this test, 50 messages with schema v1 are written to the source topic, followed by 50 messages with schema v2.
 * Schema v2 contains a forward-incompatible change, i.e. messages written with v2 cannot be read with v1.
 *
 * The first run is configured as a long-running job (writer.common.trigger.type=ProcessingTime) and with a maximum
 * number of messages per micro-batch set to 20 (reader.option.maxOffsetsPerTrigger=20). Furthermore, the schema id is
 * explicitly set for v1 (see transformer.[avro.decoder].value.schema.id). Due to the forward-incompatible change,
 * it will fail at the 51st message, which was written with schema v2. At this point, 2 micro-batches (i.e. 40 messages)
 * have been successfully committed, while the 3rd has failed half-way through. 50 messages have been written
 * to the destination topic.
 *
 * To successfully rerun, the schema id needs to be set to use schema v2. In order to avoid an infinite runtime, the
 * trigger is set to Once. The Deduplication transformer ensures that the 41st-50th messages are not written to the
 * destination topic again. In this test, offset and partition from the source topic are used as a composite id
 * to identify messages across the topics (See transformer.[kafka.deduplicator].source.id.columns
 * and transformer.[kafka.deduplicator].destination.id.columns)
 *
 * Finally, the destination topic is expected to contain all messages from the source topic
 * exactly once. Without the deduplication transformer, the 41st-50th messages would be duplicated.
 */
class KafkaToKafkaDeduplicationAfterRetryDockerTest extends FlatSpec with Matchers with SparkTestBase with BeforeAndAfter {
  import scala.collection.JavaConverters._

  private val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  private val baseDir = TempDirectory("hyperdriveE2eTest").deleteOnExit()
  private val baseDirPath = baseDir.path.toUri
  private val checkpointDir = s"$baseDirPath/checkpoint"

  behavior of "CommandLineIngestionDriver"

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
    val sourceTopicPartitions = 5
    val destinationTopicPartitions = 3
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
      "transformer.[kafka.deduplicator].destination.id.columns" -> "hyperdrive_id.source_offset, hyperdrive_id.source_partition",
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

    // when, then
    var exceptionWasThrown = false
    try {
      CommandLineIngestionDriver.main(driverConfigArray)
    } catch {
      case _: IngestionException =>
        exceptionWasThrown = true
        val consumer = createConsumer(kafkaSchemaRegistryWrapper)
        consumer.subscribe(Collections.singletonList(destinationTopic))
        val records = consumer.poll(Duration.ofMillis(1000L)).asScala.toList
        records.size shouldBe recordsV1.size
        val retryConfig = driverConfig ++ Map(
          "transformer.[avro.decoder].value.schema.id" -> s"$schemaV2Id",
          "writer.common.trigger.type" -> "Once",
          "reader.option.maxOffsetsPerTrigger" -> "9999"
        )
        val retryConfigArray = retryConfig.map { case (key, value) => s"$key=$value"}.toArray
        CommandLineIngestionDriver.main(retryConfigArray) // first rerun only retries the failed micro-batch
        CommandLineIngestionDriver.main(retryConfigArray) // second rerun consumes the rest of the messages
    }

    exceptionWasThrown shouldBe true
    fs.exists(new Path(s"$checkpointDir/$sourceTopic")) shouldBe true

    val consumer = createConsumer(kafkaSchemaRegistryWrapper)
    consumer.subscribe(Collections.singletonList(destinationTopic))
    val records = consumer.poll(Duration.ofMillis(1000L)).asScala.toList

    val valueFieldNames = records.head.value().getSchema.getFields.asScala.map(_.name())
    valueFieldNames should contain theSameElementsAs List("record_id", "value_field", "hyperdrive_id")
    val actualRecordIds = records.map(_.value().get("record_id"))
    val expectedRecordIds = 0 until recordsV1.size + recordsV2.size
    actualRecordIds should contain theSameElementsAs expectedRecordIds
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
