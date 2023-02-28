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
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.abris.avro.read.confluent.SchemaManagerFactory
import za.co.absa.commons.io.TempDirectory
import za.co.absa.spark.commons.test.SparkTestBase
import za.co.absa.abris.avro.registry.SchemaSubject
import za.co.absa.hyperdrive.ingestor.implementation.transformer.deduplicate.kafka.PrunedConsumerRecord
import za.co.absa.hyperdrive.ingestor.implementation.utils.KafkaUtil
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
 * exactly once, thanks to the deduplication transformer (see test case 1).
 * Without the deduplication transformer, the 41st-50th messages are duplicated (see test case 2).
 */
class KafkaToKafkaDeduplicationAfterRetryDockerTest extends AnyFlatSpec with Matchers with SparkTestBase with BeforeAndAfter {
  import scala.collection.JavaConverters._

  private val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  private var baseDir: TempDirectory = _
  private val pruningFn = (r: ConsumerRecord[GenericRecord, GenericRecord]) => PrunedConsumerRecord(
    r.topic(),
    r.partition(),
    r.offset(),
    Seq(r.value().get("record_id"))
  )

  behavior of "CommandLineIngestionDriver"

  it should "write exactly-once using the deduplicate transformer" in {
    val recordIdsV1 = 0 until 50
    val recordIdsV2 = 50 until 100
    val deduplicatorConfig = Map(
      "component.transformer.id.2" -> "[kafka.deduplicator]",
      "component.transformer.class.[kafka.deduplicator]" -> "za.co.absa.hyperdrive.ingestor.implementation.transformer.deduplicate.kafka.DeduplicateKafkaSinkTransformer",
      "transformer.[kafka.deduplicator].source.id.columns" -> "offset,partition",
      "transformer.[kafka.deduplicator].destination.id.columns" -> "value.hyperdrive_id.source_offset, value.hyperdrive_id.source_partition"
    )
    val kafkaSchemaRegistryWrapper = new KafkaSchemaRegistryWrapper
    val destinationTopic = "deduplication_dest"

    executeTestCase(deduplicatorConfig, recordIdsV1, recordIdsV2, kafkaSchemaRegistryWrapper, destinationTopic)

    val consumer = createConsumer(kafkaSchemaRegistryWrapper)
    val valueFieldNames = getValueSchema(consumer, destinationTopic).getFields.asScala.map(_.name())
    val consumer2 = createConsumer(kafkaSchemaRegistryWrapper)
    val records = getAllMessages(consumer2, destinationTopic, pruningFn)
    val actualRecordIds = records.flatMap(_.data.map(_.asInstanceOf[Int]))
    valueFieldNames should contain theSameElementsAs List("record_id", "value_field", "hyperdrive_id")
    actualRecordIds.distinct.size shouldBe actualRecordIds.size
    actualRecordIds should contain theSameElementsAs recordIdsV1 ++ recordIdsV2
  }

  it should "write duplicate entries without the deduplicate transformer" in {
    val recordIdsV1 = 0 until 50
    val recordIdsV2 = 50 until 100
    val kafkaSchemaRegistryWrapper = new KafkaSchemaRegistryWrapper
    val destinationTopic = "deduplication_dest"

    executeTestCase(Map(), recordIdsV1, recordIdsV2, kafkaSchemaRegistryWrapper, destinationTopic)

    val consumer = createConsumer(kafkaSchemaRegistryWrapper)
    val valueFieldNames = getValueSchema(consumer, destinationTopic).getFields.asScala.map(_.name())
    val consumer2 = createConsumer(kafkaSchemaRegistryWrapper)
    val records = getAllMessages(consumer2, destinationTopic, pruningFn)
    val actualRecordIds = records.flatMap(_.data)
    valueFieldNames should contain theSameElementsAs List("record_id", "value_field", "hyperdrive_id")
    actualRecordIds.distinct.size should be < actualRecordIds.size
  }

  // scalastyle:off method.length
  private def executeTestCase(deduplicatorConfig: Map[String, String], recordIdsV1: Seq[Int], recordIdsV2: Seq[Int],
                              kafkaSchemaRegistryWrapper: KafkaSchemaRegistryWrapper, destinationTopic: String) = {
    // given
    val checkpointDir = s"${baseDir.path.toUri}/checkpoint"
    val sourceTopic = "deduplication_src"
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

    val recordsV1 = recordIdsV1.map(i => {
      val valueRecord = new GenericData.Record(schemaV1)
      valueRecord.put("record_id", i)
      valueRecord.put("value_field", s"valueHello_$i")
      valueRecord
    })
    val recordsV2 = recordIdsV2.map(i => {
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
      "component.transformer.id.3" -> "[avro.encoder]",
      "component.transformer.class.[avro.encoder]" -> "za.co.absa.hyperdrive.ingestor.implementation.transformer.avro.confluent.ConfluentAvroEncodingTransformer",
      "component.writer" -> "za.co.absa.hyperdrive.ingestor.implementation.writer.kafka.KafkaStreamWriter",

      // Spark settings
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

      // Avro Encoder (ABRiS) settings
      "transformer.[avro.encoder].schema.registry.url" -> "${transformer.[avro.decoder].schema.registry.url}",
      "transformer.[avro.encoder].value.schema.naming.strategy" -> "topic.name",

      // Sink(Kafka) settings
      "writer.common.checkpoint.location" -> (checkpointDir + "/${reader.kafka.topic}"),
      "writer.common.trigger.type" -> "ProcessingTime",
      "writer.kafka.topic" -> destinationTopic,
      "writer.kafka.brokers" -> "${reader.kafka.brokers}"
    ) ++ deduplicatorConfig
    val driverConfigArray = driverConfig.map { case (key, value) => s"$key=$value" }.toArray

    // when, then
    var exceptionWasThrown = false
    try {
      CommandLineIngestionDriver.main(driverConfigArray)
    } catch {
      case _: IngestionException =>
        exceptionWasThrown = true
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
  }
  // scalastyle:on method.length

  before {
    baseDir = TempDirectory("hyperdriveE2eTest").deleteOnExit()
  }

  after {
    SchemaManagerFactory.resetSRClientInstance()
    baseDir.delete()
  }

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
    localKafkaAdmin.createTopics(util.Arrays.asList(topic)).all().get()
  }

  private def createProducer(kafkaSchemaRegistryWrapper: KafkaSchemaRegistryWrapper): KafkaProducer[GenericRecord, GenericRecord] = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaSchemaRegistryWrapper.kafka.getBootstrapServers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer")
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaToKafkaProducer")
    props.put(ProducerConfig.ACKS_CONFIG, "1")
    kafkaSchemaRegistryWrapper.createProducer(props)
  }

  private def createConsumer(kafkaSchemaRegistryWrapper: KafkaSchemaRegistryWrapper): KafkaConsumer[GenericRecord, GenericRecord] = {
    import org.apache.kafka.clients.consumer.ConsumerConfig
    val props = new Properties()
    props.put(ConsumerConfig.GROUP_ID_CONFIG, randomUUID.toString)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaSchemaRegistryWrapper.kafka.getBootstrapServers)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer")
    kafkaSchemaRegistryWrapper.createConsumer(props)
  }

  private def getValueSchema(consumer: KafkaConsumer[GenericRecord, GenericRecord], topic: String) = {
    consumer.subscribe(Seq(topic).asJava)
    consumer.poll(Duration.ofSeconds(10L)).asScala.head.value().getSchema
  }

  private def getAllMessages[K, V](consumer: KafkaConsumer[K, V], topic: String, pruningFn: ConsumerRecord[K, V] => PrunedConsumerRecord) = {
    val topicPartitions = KafkaUtil.getTopicPartitions(consumer, topic)
    val offsets = consumer.endOffsets(topicPartitions.asJava)
    implicit val kafkaConsumerTimeout: Duration = Duration.ofSeconds(10L)
    KafkaUtil.getMessagesAtLeastToOffset(consumer, offsets.asScala.mapValues(Long2long).toMap, pruningFn)
  }
}
