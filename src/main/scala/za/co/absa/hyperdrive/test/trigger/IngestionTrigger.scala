/*
 *
 * Copyright 2019 ABSA Group Limited
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package za.co.absa.hyperdrive.test.trigger

import java.util.Collections

import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import za.co.absa.hyperdrive.test.producer.notification.{Notification, NotificationDispatcher}
import org.apache.kafka.clients.consumer.KafkaConsumer
import za.co.absa.abris.avro.read.confluent.{ScalaConfluentKafkaAvroDeserializer, SchemaManager}
import za.co.absa.abris.avro.schemas.policy.SchemaRetentionPolicies
import za.co.absa.hyperdrive.ingestion.SparkIngestor
import za.co.absa.hyperdrive.offset.OffsetManager
import za.co.absa.hyperdrive.offset.impl.CheckpointingOffsetManager
import za.co.absa.hyperdrive.readers.StreamReader
import za.co.absa.hyperdrive.readers.impl.KafkaStreamReader
import za.co.absa.hyperdrive.settings.InfrastructureSettings._
import za.co.absa.hyperdrive.test.utils.PayloadPrinter
import za.co.absa.hyperdrive.transformations.data.StreamTransformer
import za.co.absa.hyperdrive.transformations.data.impl.SelectorStreamTransformer
import za.co.absa.hyperdrive.transformations.encoding.AvroDecoder
import za.co.absa.hyperdrive.transformations.encoding.schema.impl.SchemaRegistrySchemaPathProvider
import za.co.absa.hyperdrive.writers.StreamWriter
import za.co.absa.hyperdrive.writers.impl.ParquetStreamWriter

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
    consumer.partitionsFor(HyperdriveSettings.NOTIFICATION_TOPIC).asScala.foreach(println)

    consumer.seekToBeginning(consumer.assignment())

    while(true){
      val records = consumer.poll(100)

      for (record <-records.asScala) {
        val avroRecord = decode(record.value.asInstanceOf[Array[Byte]])
        val notification = toNotification(avroRecord)
        println(s"Received: $notification. Invoking ingestion")

        val payloadTopic = notification.topic
        val destinationDir = notification.destinationDir

        val streamReader = createStreamReader(payloadTopic)
        val offsetManager = createOffsetManager(payloadTopic)
        val avroDecoder = createAvroDecoder(payloadTopic)
        val streamTransformer = createStreamTransformer
        val streamWriter = createStreamWriter(destinationDir)

        SparkIngestor.ingest(payloadTopic)(streamReader)(offsetManager)(avroDecoder)(streamTransformer)(streamWriter)

        PayloadPrinter.showContent(destinationDir, PayloadPrinter.FORMAT_PARQUET)
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

  private def createStreamReader(topic: String): StreamReader = {
    return new KafkaStreamReader(topic, KafkaSettings.BROKERS)
  }

  private def createOffsetManager(topic: String): OffsetManager = {
    new CheckpointingOffsetManager(topic, new Configuration())
  }

  private def createAvroDecoder(topic: String): AvroDecoder = {
    val schemaRegistrySettings = getSchemaRegistrySettings(topic)
    val schemaPathProvider = new SchemaRegistrySchemaPathProvider(schemaRegistrySettings)
    new AvroDecoder(schemaPathProvider, SchemaRetentionPolicies.RETAIN_SELECTED_COLUMN_ONLY)
  }

  private def getSchemaRegistrySettings(topic: String): Map[String,String] = {
    // attaching topic and schema id to general Schema Registry settings
    SchemaRegistrySettings.SCHEMA_REGISTRY_ACCESS_SETTINGS + (SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> topic, SchemaManager.PARAM_VALUE_SCHEMA_ID -> "latest")
  }

  private def createStreamTransformer: StreamTransformer = new SelectorStreamTransformer

  private def createStreamWriter(destination: String): StreamWriter = {
    new ParquetStreamWriter(destination)
  }
}
