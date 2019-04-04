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

package za.co.absa.hyperdrive.trigger

import java.util.Collections

import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.SparkSession
import za.co.absa.abris.avro.read.confluent.{ScalaConfluentKafkaAvroDeserializer, SchemaManager}
import za.co.absa.abris.avro.schemas.policy.SchemaRetentionPolicies
import za.co.absa.hyperdrive.ingestor.SparkIngestor
import za.co.absa.hyperdrive.manager.offset.OffsetManager
import za.co.absa.hyperdrive.manager.offset.impl.CheckpointingOffsetManager
import za.co.absa.hyperdrive.reader.StreamReader
import za.co.absa.hyperdrive.reader.impl.KafkaStreamReader
import za.co.absa.hyperdrive.shared.InfrastructureSettings.{HyperdriveSettings, KafkaSettings, SchemaRegistrySettings, SparkSettings}
import za.co.absa.hyperdrive.transformer.data.StreamTransformer
import za.co.absa.hyperdrive.transformer.data.impl.SelectAllStreamTransformer
import za.co.absa.hyperdrive.transformer.encoding.impl.AvroDecoder
import za.co.absa.hyperdrive.trigger.mock.NotificationDispatcher
import za.co.absa.hyperdrive.trigger.notification.Notification
import za.co.absa.hyperdrive.trigger.utils.PayloadPrinter
import za.co.absa.hyperdrive.writer.StreamWriter
import za.co.absa.hyperdrive.writer.impl.ParquetStreamWriter

import scala.collection.JavaConverters._

object IngestionTrigger {

  private val logger = LogManager.getLogger

  def main(args: Array[String]): Unit = {

    resolveBrokersAndSchemaRegistry(args)

    import java.util.Properties

    val  props = new Properties()
    props.put(KafkaSettings.BROKERS_SETTING_KEY, KafkaSettings.BROKERS)
    props.put(KafkaSettings.KEY_DESERIALIZER_KEY, KafkaSettings.KEY_DESERIALIZER)
    props.put(KafkaSettings.VALUE_DESERIALIZER_KEY, KafkaSettings.VALUE_DESERIALIZER)
    props.put(KafkaSettings.GROUP_ID_KEY, "SomeGroupId")
    props.put("enable.auto.commit", "true")

    logger.info(s"STARTING notification topic watcher: '${HyperdriveSettings.NOTIFICATION_TOPIC}'")

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

        val sparkSession = getSparkSession(payloadTopic)
        sparkSession.conf.getAll.foreach(println)

        val runnable = new Runnable {
          override def run(): Unit = {
            SparkIngestor.ingest(getSparkSession(payloadTopic))(streamReader)(offsetManager)(avroDecoder)(streamTransformer)(streamWriter)
            PayloadPrinter.showContent(destinationDir, PayloadPrinter.FORMAT_PARQUET)
          }
        }

        new Thread(runnable).start()
      }
    }
  }

  private def resolveBrokersAndSchemaRegistry(args: Array[String]): Unit = {
    if (args.nonEmpty) {
      KafkaSettings.BROKERS = args(0).trim
      SchemaRegistrySettings.URL = args(1).trim
    }
    logger.info(s"Kaka broker resolved to: ${KafkaSettings.BROKERS}")
    logger.info(s"Schema Registry broker resolved to: ${SchemaRegistrySettings.URL}")
  }

  private def getSparkSession(topic: String): SparkSession = {
    SparkSession
      .builder()
      .appName(s"SparkIngestor-$topic")
      .master("local[*]")
      .getOrCreate()
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
    new KafkaStreamReader(topic, KafkaSettings.BROKERS, Map[String,String]())
  }

  private def createOffsetManager(topic: String): OffsetManager = {
    new CheckpointingOffsetManager(topic, SparkSettings.CHECKPOINT_BASE_LOCATION, new Configuration())
  }

  private def createAvroDecoder(topic: String): AvroDecoder = {
    new AvroDecoder(getSchemaRegistrySettings(topic), SchemaRetentionPolicies.RETAIN_SELECTED_COLUMN_ONLY)
  }

  private def getSchemaRegistrySettings(topic: String): Map[String,String] = {
    // attaching topic and schema id to general Schema Registry settings
    SchemaRegistrySettings.SCHEMA_REGISTRY_ACCESS_SETTINGS + (SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> topic, SchemaManager.PARAM_VALUE_SCHEMA_ID -> "latest")
  }

  private def createStreamTransformer: StreamTransformer = new SelectAllStreamTransformer

  private def createStreamWriter(destination: String): StreamWriter = {
    new ParquetStreamWriter(destination)
  }
}
