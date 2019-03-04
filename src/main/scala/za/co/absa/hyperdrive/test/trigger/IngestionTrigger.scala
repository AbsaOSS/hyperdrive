/*
 * Copyright 2018 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.hyperdrive.test.trigger

import java.util.Collections

import org.apache.avro.generic.GenericRecord
import za.co.absa.hyperdrive.test.producer.notification.{Notification, NotificationDispatcher}
import org.apache.kafka.clients.consumer.KafkaConsumer
import za.co.absa.abris.avro.read.confluent.ScalaConfluentKafkaAvroDeserializer
import za.co.absa.hyperdrive.test.ingestion.SparkIngestor
import za.co.absa.hyperdrive.test.settings.InfrastructureSettings._
import za.co.absa.hyperdrive.test.utils.PayloadPrinter

import scala.collection.JavaConverters._

object IngestionTrigger {

  def main(args: Array[String]): Unit = {

    import java.util.Properties

    val  props = new Properties()
    props.put(KafkaSettings.BROKERS_SETTING_KEY, KafkaSettings.BROKERS)
    props.put(KafkaSettings.KEY_DESERIALIZER_KEY, KafkaSettings.KEY_DESERIALIZER)
    props.put(KafkaSettings.VALUE_DESERIALIZER_KEY, KafkaSettings.VALUE_DESERIALIZER)
    props.put(KafkaSettings.GROUP_ID_KEY, "something")
    props.put("enable.auto.commit", "true")

    println(s"STARTING notification topic watcher: '${HyperdriveSettings.NOTIFICATION_TOPIC}'")

    val consumer = new KafkaConsumer[String, String](props)

    consumer.subscribe(Collections.singletonList(HyperdriveSettings.NOTIFICATION_TOPIC))

    consumer.assignment().asScala.foreach(println)

    consumer.seekToBeginning(consumer.assignment())

    while(true){
      val records = consumer.poll(100)

      for (record <-records.asScala) {
        val avroRecord = decode(record.value.asInstanceOf[Array[Byte]])
        val notification = toNotification(avroRecord)
        println(s"Received: $notification. Invoking ingestion")

        SparkIngestor.ingest(notification.topic, notification.destinationDir)

        PayloadPrinter.showContent(notification.destinationDir, PayloadPrinter.FORMAT_PARQUET)
      }
    }
  }

  private def decode(payload: Array[Byte]): GenericRecord = {
    val reader = new ScalaConfluentKafkaAvroDeserializer(Option(HyperdriveSettings.NOTIFICATION_TOPIC), Option.empty)
    reader.configureSchemaRegistry(NotificationDispatcher.getSchemaRegistrySettings)
    reader.deserialize(payload)
  }

  private def toNotification(record: GenericRecord): Notification = {
    val topic = record.get("topic").asInstanceOf[String]
    val destinationDir = record.get("destinationDir").asInstanceOf[String]

    Notification(topic, destinationDir)
  }
}
