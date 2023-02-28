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

package za.co.absa.hyperdrive.ingestor.implementation.utils

import java.time.Duration
import java.util
import java.util.UUID.randomUUID
import java.util.{Collections, Properties}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.scalatest.{AppendedClues, BeforeAndAfter}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName
import za.co.absa.hyperdrive.ingestor.implementation.transformer.deduplicate.kafka.PrunedConsumerRecord

import scala.collection.JavaConverters._
import scala.collection.mutable
class TestKafkaUtilDockerTest extends AnyFlatSpec with Matchers with BeforeAndAfter with AppendedClues {

  private val confluentPlatformVersion = "5.3.4" // should be same as kafka.avro.serializer.version property in pom file
  private val kafka = new KafkaContainer(DockerImageName.parse(s"confluentinc/cp-kafka:$confluentPlatformVersion"))
  private val kafkaSufficientTimeout = Duration.ofSeconds(5L)
  private val kafkaInsufficientTimeout = Duration.ofMillis(1L)
  private val topic = "test-topic"
  private val maxPollRecords = 10
  private val pruningFn = (r: ConsumerRecord[String, String]) => PrunedConsumerRecord(
    r.topic(),
    r.partition(),
    r.offset(),
    Seq(r.value())
  )

  before{
    kafka.start()
  }

  after {
    kafka.stop()
  }

  "getMessagesAtLeastToOffset" should "get all available messages" in {
    // given
    val partitions = 3
    createTopic(kafka, topic, partitions)
    val producer = createProducer(kafka)
    val messages = (1 to 100).map(i => s"message_${i}")
    produceData(producer, messages, topic, partitions)

    val consumer = createConsumer(kafka)
    val topicPartitions = KafkaUtil.getTopicPartitions(consumer, topic)
    val offsets = consumer.endOffsets(topicPartitions.asJava).asScala.toMap.mapValues(_.asInstanceOf[Long])

    // when
    implicit val kafkaConsumerTimeout: Duration = kafkaSufficientTimeout
    val records = KafkaUtil.getMessagesAtLeastToOffset(consumer, offsets, pruningFn)

    // then
    val actualMessages = records.map(_.data.head.asInstanceOf[String]).toList.sorted
    actualMessages should contain theSameElementsAs messages
  }

  it should "stop polling when the desired end offset has been reached and not run infinitely" in {
    // given
    val partitions = 3
    createTopic(kafka, topic, partitions)
    val producer = createProducer(kafka)
    val messages = (1 to 100).map(i => s"message_${i}")
    produceData(producer, messages, topic, partitions)

    val infiniteProducerThread = new Thread {
      override def run {
        var i = 0
        while (true) {
          val partition = i % partitions
          val producerRecord = new ProducerRecord[String, String](topic, partition, null, s"message_${i}")
          producer.send(producerRecord)
          i += 1
          if (i % 100 == 0) {
            producer.flush()
          }
        }
      }
    }

    val consumer = createConsumer(kafka)
    val topicPartitions = KafkaUtil.getTopicPartitions(consumer, topic)
    val offsets = consumer.endOffsets(topicPartitions.asJava).asScala.toMap.mapValues(_.asInstanceOf[Long])
    infiniteProducerThread.start()

    // when
    implicit val kafkaConsumerTimeout: Duration = kafkaSufficientTimeout
    val records = KafkaUtil.getMessagesAtLeastToOffset(consumer, offsets, pruningFn)

    // then
    val actualMessages = records.map(_.data.head.asInstanceOf[String]).toList.sorted
    actualMessages should contain allElementsOf messages

    // cleanup
    infiniteProducerThread.interrupt()
  }

  it should "throw an exception if consumer is already subscribed" in {
    // given
    createTopic(kafka, topic, 1)

    val consumer = createConsumer(kafka)
    consumer.subscribe(Collections.singletonList(topic))

    // when
    implicit val kafkaConsumerTimeout: Duration = kafkaInsufficientTimeout
    val exception = the[Exception] thrownBy KafkaUtil.getMessagesAtLeastToOffset(consumer, Map(new TopicPartition(topic, 0) -> 0), pruningFn)

    // then
    exception.getMessage should include ("Subscription to topics, partitions and pattern are mutually exclusive")
  }

  it should "throw an exception if not all messages could be consumed (because the timeout is too short)" in {
    // given
    val partitions = 3
    createTopic(kafka, topic, partitions)
    val producer = createProducer(kafka)
    val messages = (1 to 100).map(i => s"message_${i}")
    produceData(producer, messages, topic, partitions)

    val consumer = createConsumer(kafka)
    val topicPartitions = KafkaUtil.getTopicPartitions(consumer, topic)
    consumer.assign(topicPartitions.asJava)
    consumer.seekToBeginning(topicPartitions.asJava)
    val offsets = consumer.endOffsets(topicPartitions.asJava).asScala.toMap.mapValues(_.asInstanceOf[Long])

    // when
    implicit val kafkaConsumerTimeout: Duration = kafkaInsufficientTimeout
    val exception = the[Exception] thrownBy KafkaUtil.getMessagesAtLeastToOffset(consumer, offsets, pruningFn)

    // then
    exception.getMessage should include ("Not all expected messages were consumed")
  }

  it should "throw an exception if requested offsets are not available" in {
    // given
    val partitions = 3
    createTopic(kafka, topic, partitions)
    val producer = createProducer(kafka)
    val messages = (1 to 100).map(i => s"message_${i}")
    produceData(producer, messages, topic, partitions)

    val consumer = createConsumer(kafka)
    val topicPartitions = KafkaUtil.getTopicPartitions(consumer, topic)
    val offsets = consumer.endOffsets(topicPartitions.asJava).asScala.toMap.mapValues(_ * 2L)

    // when
    implicit val kafkaConsumerTimeout: Duration = kafkaInsufficientTimeout
    val exception = the[Exception] thrownBy KafkaUtil.getMessagesAtLeastToOffset(consumer, offsets, pruningFn)

    // then
    exception.getMessage should include ("Requested consumption")
  }

  "getAtLeastNLatestRecords" should "get at least the n latest records if there are gaps in the offsets" in {
    val messageCreationTimeout = 100L
    val partitions = 3
    createTopic(kafka, topic, partitions, Map(
      "cleanup.policy" -> "compact",
      "delete.retention.ms" -> "100",
      "segment.ms" -> s"$messageCreationTimeout",
      "min.cleanable.dirty.ratio" -> "0.01"
    ))

    val producer = createProducer(kafka)
    val messages = (1 to 103).map(i => {
      val key = if (i % 2 == 0 || i > 100) 1000 + i else 1
      (key.toString, s"msg_${i}", i % partitions)
    })
    produceData(producer, messages, topic, Some(messageCreationTimeout))

    val waitForCompactionMillis = 20000L
    Thread.sleep(waitForCompactionMillis)

    val testConsumer = createConsumer(kafka)
    testConsumer.subscribe(Collections.singletonList(topic))
    import scala.util.control.Breaks._
    var records: Seq[ConsumerRecord[String, String]] = mutable.Seq()
    breakable {
      while (true) {
        val newRecords = testConsumer.poll(kafkaSufficientTimeout).asScala.toSeq
        records ++= newRecords
        if (newRecords.isEmpty) {
          break()
        }
      }
    }

    withClue(){
      records.size shouldBe messages.map(r => (r._1, r._3)).distinct.size
    } withClue(s"This is likely an artifact of the test itself. You may want to increase waitForCompactionMillis." +
      s" The current value is $waitForCompactionMillis")

    val consumer = createConsumer(kafka)
    implicit val kafkaConsumerTimeout: Duration = kafkaSufficientTimeout
    val topicPartitions = KafkaUtil.getTopicPartitions(consumer, topic)
    val recordsPerPartition = topicPartitions.map(p => p -> 4L).toMap
    val actualRecords = KafkaUtil.getAtLeastNLatestRecordsFromPartition(consumer, recordsPerPartition, pruningFn)
    val values = actualRecords.map(_.data.head.asInstanceOf[String])

    values.size should be >= 12
    values should contain allElementsOf Seq("msg_103", "msg_102", "msg_101", "msg_100", "msg_99", "msg_97", "msg_95",
      "msg_98", "msg_96", "msg_94", "msg_92", "msg_90")
  }

  it should "get from multiple topics simultaneously" in {
    // given
    val partitions = 3
    createTopic(kafka, topic, partitions)
    val producer = createProducer(kafka)
    val messages = (1 to 100).map(i => s"message_${i}")
    produceData(producer, messages, topic, partitions)

    val consumer = createConsumer(kafka)
    val topicPartitions = KafkaUtil.getTopicPartitions(consumer, topic)

    // when
    implicit val kafkaConsumerTimeout: Duration = kafkaSufficientTimeout
    val recordsPerPartition = topicPartitions.map(t => t -> 1000L).toMap
    val records = KafkaUtil.getAtLeastNLatestRecordsFromPartition(consumer, recordsPerPartition, pruningFn)

    // then
    val actualMessages = records.map(_.data.head.asInstanceOf[String]).toList.sorted
    actualMessages should contain theSameElementsAs messages
  }

  it should "throw an exception if the timeout is too short" in {
    createTopic(kafka, topic, 1)

    val producer = createProducer(kafka)
    val messages = (1 to 100).map(i => s"message_${i}")

    produceData(producer, messages, topic, 1)

    val consumer = createConsumer(kafka)
    implicit val kafkaConsumerTimeout: Duration = kafkaInsufficientTimeout
    val result = the[Exception] thrownBy KafkaUtil.getAtLeastNLatestRecordsFromPartition(consumer,
      Map(new TopicPartition(topic, 0) -> 10), pruningFn)
    result.getMessage should include("increasing the consumer timeout")
  }

  "getTopicPartitions" should "return the partitions" in {
    createTopic(kafka, topic, 10)
    val consumer = createConsumer(kafka)

    val topicPartitions = KafkaUtil.getTopicPartitions(consumer, topic)

    val expectedPartitions = (0 until 10).map(i => new TopicPartition(topic, i))
    topicPartitions should contain theSameElementsAs expectedPartitions
  }

  "seekToOffsetsOrBeginning" should "seek to the provided offsets" in {
    // given
    createTopic(kafka, topic, 3)
    val consumer = createConsumer(kafka)

    val producer = createProducer(kafka)
    val messages = (1 to 100).map(i => s"message_${i}")
    produceData(producer, messages, topic, 3)

    val tp0 = new TopicPartition(topic, 0)
    val tp1 = new TopicPartition(topic, 1)
    val tp2 = new TopicPartition(topic, 2)
    val offsets = Map(
      tp0 -> 10L,
      tp1 -> 15L,
      tp2 -> 20L
    )

    // when
    KafkaUtil.seekToOffsetsOrBeginning(consumer, topic, Some(offsets))

    // then
    consumer.position(tp0) shouldBe 10L
    consumer.position(tp1) shouldBe 15L
    consumer.position(tp2) shouldBe 20L
  }

  it should "seek to the beginning if no offsets are given" in {
    // given
    createTopic(kafka, topic, 3)
    val consumer = createConsumer(kafka)

    val producer = createProducer(kafka)
    val messages = (1 to 100).map(i => s"message_${i}")
    produceData(producer, messages, topic, 3)

    val tp0 = new TopicPartition(topic, 0)
    val tp1 = new TopicPartition(topic, 1)
    val tp2 = new TopicPartition(topic, 2)

    // when
    KafkaUtil.seekToOffsetsOrBeginning(consumer, topic, None)

    // then
    consumer.position(tp0) shouldBe 0L
    consumer.position(tp1) shouldBe 0L
    consumer.position(tp2) shouldBe 0L
  }

  private def createTopic(kafkaContainer: KafkaContainer, topicName: String, partitions: Int, extraConfig: Map[String, String] = Map()): Unit = {
    val config = new Properties()
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers)
    val localKafkaAdmin = AdminClient.create(config)
    val replication = 1.toShort
    val topic = new NewTopic(topicName, partitions, replication).configs(extraConfig.asJava)
    val topicCreationFut = localKafkaAdmin.createTopics(util.Arrays.asList(topic)).all()
    topicCreationFut.get()
  }

  def createProducer(kafkaContainer: KafkaContainer): KafkaProducer[String, String] = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, randomUUID().toString)
    props.put(ProducerConfig.ACKS_CONFIG, "1")
    new KafkaProducer[String, String](props)
  }

  def createConsumer(kafkaContainer: KafkaContainer): KafkaConsumer[String, String] = {
    import org.apache.kafka.clients.consumer.ConsumerConfig
    val props = new Properties()
    props.put(ConsumerConfig.GROUP_ID_CONFIG, randomUUID.toString)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords.toString)
    new KafkaConsumer[String, String](props)
  }

  private def produceData(producer: KafkaProducer[String, String], valueRecords: Seq[String], topic: String, partitions: Int): Unit = {
    val records = valueRecords.zipWithIndex.map {
      case (value, i) => (null, value, i % partitions)
    }
    produceData(producer, records, topic)
  }

  private def produceData(producer: KafkaProducer[String, String], records: Seq[(String, String, Int)], topic: String,
    timeout: Option[Long] = None): Unit = {
    records.foreach {
      record =>
        val producerRecord = new ProducerRecord[String, String](topic, record._3, record._1, record._2)
        producer.send(producerRecord)
        timeout.foreach(Thread.sleep)
    }
    producer.flush()
  }
}


