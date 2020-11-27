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
import java.util.{Collections, Properties, UUID}

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{Deserializer, Serializer, StringDeserializer, StringSerializer}
import org.scalatest.{Assertion, BeforeAndAfter, FlatSpec, Matchers}
import org.testcontainers.containers.KafkaContainer

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
    createTopic(kafka, "get-all-available-messages-topic", partitions)
    val producer = createProducer(kafka)
    val messagesCount = 10000
    val messages = (1 to messagesCount).map(i => s"message_${i}")
    produceData(producer, messages, topic, partitions)

    val consumer = createConsumer(kafka)
    consumer.subscribe(Collections.singletonList(topic))

    // when
    implicit val kafkaConsumerTimeout: Duration = Duration.ofMillis(500L)
    val records = KafkaUtil.getAllAvailableMessages(consumer)

    // then
    val actualMessages = records.map(_.value())
    actualMessages should contain theSameElementsAs (1 to messagesCount).map(i => s"message_$i")
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
  }

}


