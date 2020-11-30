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
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.testcontainers.containers.KafkaContainer

import scala.collection.JavaConverters._
class TestKafkaUtilDockerTest extends FlatSpec with Matchers with BeforeAndAfter {

  private val confluentPlatformVersion = "5.3.4" // should be same as kafka.avro.serializer.version property in pom file
  private val kafka = new KafkaContainer(confluentPlatformVersion)

  before{
    kafka.start()
  }

  after {
    kafka.stop()
  }

  "getAllAvailableMessages" should "get all available messages" in {
    // given
    val topic = "get-all-available-messages-topic"
    val partitions = 3
    createTopic(kafka, topic, partitions)
    val producer = createProducer(kafka)
    val messages = (1 to 100).map(i => s"message_${i}")
    produceData(producer, messages, topic, partitions)

    val consumer = createConsumer(kafka)
    consumer.subscribe(Collections.singletonList(topic))
    val topicPartitions = KafkaUtil.getTopicPartitions(consumer, topic)
    val offsets = consumer.endOffsets(topicPartitions.asJava).asScala.toMap.mapValues(_.asInstanceOf[Long])

    // when
    implicit val kafkaConsumerTimeout: Duration = Duration.ofMillis(1500L)
    val records = KafkaUtil.getMessagesAtLeastToOffset(consumer, offsets)

    // then
    val actualMessages = records.map(_.value()).toList.sorted
    actualMessages should contain theSameElementsAs messages
  }

  it should "throw an exception if partitions were not assigned" in {
    // given
    val topic = "get-all-available-messages-topic"
    val partitions = 3
    createTopic(kafka, topic, partitions)
    val producer = createProducer(kafka)
    val messages = (1 to 100).map(i => s"message_${i}")
    produceData(producer, messages, topic, partitions)

    val consumer = createConsumer(kafka)
    consumer.subscribe(Collections.singletonList(topic))
    val topicPartitions = KafkaUtil.getTopicPartitions(consumer, topic)
    val offsets = consumer.endOffsets(topicPartitions.asJava).asScala.toMap.mapValues(_.asInstanceOf[Long])

    // when
    implicit val kafkaConsumerTimeout: Duration = Duration.ofMillis(1L)
    val exception = the[Exception] thrownBy KafkaUtil.getMessagesAtLeastToOffset(consumer, offsets)

    // then
    exception.getMessage should include ("Consumer is unexpectedly not assigned")
  }

  it should "throw an exception if not all messages could be consumed" in {
    // given
    val topic = "get-all-available-messages-topic"
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
    implicit val kafkaConsumerTimeout: Duration = Duration.ofMillis(1L)
    val exception = the[Exception] thrownBy KafkaUtil.getMessagesAtLeastToOffset(consumer, offsets)

    // then
    exception.getMessage should include ("Not all expected messages were consumed")
  }


  it should "throw an exception if requested offsets are not available" in {
    // given
    val topic = "get-all-available-messages-topic"
    val partitions = 3
    createTopic(kafka, topic, partitions)
    val producer = createProducer(kafka)
    val messages = (1 to 100).map(i => s"message_${i}")
    produceData(producer, messages, topic, partitions)

    val consumer = createConsumer(kafka)
    consumer.subscribe(Collections.singletonList(topic))
    val topicPartitions = KafkaUtil.getTopicPartitions(consumer, topic)
    val offsets = consumer.endOffsets(topicPartitions.asJava).asScala.toMap.mapValues(_ * 2L)

    // when
    implicit val kafkaConsumerTimeout: Duration = Duration.ofMillis(0L)
    val exception = the[Exception] thrownBy KafkaUtil.getMessagesAtLeastToOffset(consumer, offsets)

    // then
    exception.getMessage should include ("Requested consumption")
  }

  private def createTopic(kafkaContainer: KafkaContainer, topicName: String, partitions: Int): Unit = {
    val config = new Properties()
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers)
    val localKafkaAdmin = AdminClient.create(config)
    val replication = 1.toShort
    val topic = new NewTopic(topicName, partitions, replication)
    val topicCreationFut = localKafkaAdmin.createTopics(util.Arrays.asList(topic)).all()
    while(!topicCreationFut.isDone) {}
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
    new KafkaConsumer[String, String](props)
  }

  private def produceData(producer: KafkaProducer[String, String], records: Seq[String], topic: String, partitions: Int): Unit = {
    records.zipWithIndex.foreach {
      case (record, i) =>
        val partition = i % partitions
        val producerRecord = new ProducerRecord[String, String](topic, partition, null, record)
        producer.send(producerRecord)
    }
    producer.flush()
  }

}


