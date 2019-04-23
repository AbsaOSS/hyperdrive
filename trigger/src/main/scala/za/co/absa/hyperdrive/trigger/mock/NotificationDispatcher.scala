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

package za.co.absa.hyperdrive.trigger.mock

import org.apache.spark.sql.{DataFrame, SparkSession}
import za.co.absa.abris.avro.read.confluent.SchemaManager
import za.co.absa.hyperdrive.trigger.notification.Notification
import ConfigParams._

object SystemSettings {
  val JVM_DEFAULT_TMP_DIR_PROPERTY = "java.io.tmpdir"
}

object HyperdriveSettings {

  val BASE_TEMP_DIR: String = {
    val jvmTmpDir = System.getProperty(SystemSettings.JVM_DEFAULT_TMP_DIR_PROPERTY)
    val baseTmpDir = if (jvmTmpDir == null || jvmTmpDir.trim.isEmpty) "/tmp" else jvmTmpDir
    baseTmpDir + "/HYPERDRIVE_TEMP/"
  }
  val BASE_PAYLOAD_DESTINATION = "/tmp/HYPERDRIVE_PAYLOAD"

  val NOTIFICATION_TOPIC = "notification_topic"
  val PAYLOAD_TOPIC_1    = "payload_topic_1"
  val PAYLOAD_TOPIC_2    = "payload_topic_2"
  var PAYLOAD_TOPIC_IN_USE: String = PAYLOAD_TOPIC_1

  def getPayloadDestinationDir: String = {
    if (PAYLOAD_TOPIC_IN_USE == PAYLOAD_TOPIC_1)
      s"$BASE_PAYLOAD_DESTINATION/dest1"
    else if (PAYLOAD_TOPIC_IN_USE == PAYLOAD_TOPIC_2)
      s"$BASE_PAYLOAD_DESTINATION/dest2"
    else s"$BASE_PAYLOAD_DESTINATION/INCORRECT_DESTINATION"
  }
}

object KafkaSettings {
  val STREAM_FORMAT_KAFKA_NAME  = "kafka"
  val BROKERS_SETTING_KEY       = "bootstrap.servers"
  val SPARK_BROKERS_SETTING_KEY = "kafka.bootstrap.servers"

  val TOPIC_SUBSCRIPTION_KEY    = "subscribe"
  val TOPIC_DISPATCH_KEY        = "topic"

  val STARTING_OFFSETS_KEY      = "startingOffsets"
  val STARTING_OFFSETS_EARLIEST = "earliest"

  val KEY_DESERIALIZER_KEY      = "key.deserializer"
  val KEY_DESERIALIZER          = "org.apache.kafka.common.serialization.StringDeserializer"

  val VALUE_DESERIALIZER_KEY    = "value.deserializer"
  val VALUE_DESERIALIZER        = "org.apache.kafka.common.serialization.ByteArrayDeserializer"

  val GROUP_ID_KEY              = "group.id"
}

object SchemaRegistrySettings {
  val KEY_SCHEMA_NAMING_STRATEGY: String   = SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME
  val VALUE_SCHEMA_NAMING_STRATEGY: String = SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME
}

object AvroSettings {
  val GENERAL_SCHEMA_NAME      = "schemaName"
  val GENERAL_SCHEMA_NAMESPACE = "schemaNamespace"
}

object NotificationDispatcher {

  def main(args: Array[String]): Unit = {
    dispatchNotification()
  }

  def dispatchNotification(): Unit = {

    val spark = SparkSession.builder().appName("NotificationDispatcher").master("local[*]").getOrCreate()

    val notificationDf = createNotification(spark)

    println("Created notifications")
    notificationDf.show()

    println(s"Going to send ${notificationDf.count()} notifications to topic '${HyperdriveSettings.NOTIFICATION_TOPIC}'")
    dispatch(notificationDf, spark)

    println(s"Done sending ${notificationDf.count()} notifications to topic ${HyperdriveSettings.NOTIFICATION_TOPIC}")
  }

  private def createNotification(spark: SparkSession): DataFrame = {
    import spark.implicits._
    spark.sparkContext.parallelize(Seq(Notification(HyperdriveSettings.PAYLOAD_TOPIC_IN_USE, HyperdriveSettings.getPayloadDestinationDir))).toDF()
  }

  private def dispatch(notificationDf: DataFrame, sparkSession: SparkSession): Unit = {

    import za.co.absa.abris.avro.AvroSerDe._

    notificationDf
      .toConfluentAvro(HyperdriveSettings.NOTIFICATION_TOPIC, AvroSettings.GENERAL_SCHEMA_NAME, AvroSettings.GENERAL_SCHEMA_NAMESPACE)(getSchemaRegistrySettings)
      .write
      .format(KafkaSettings.STREAM_FORMAT_KAFKA_NAME)
      .option(KafkaSettings.SPARK_BROKERS_SETTING_KEY, KAFKA_BROKERS)
      .option(KafkaSettings.TOPIC_DISPATCH_KEY, HyperdriveSettings.NOTIFICATION_TOPIC)
      .save()
  }

  def getSchemaRegistrySettings: Map[String,String] = {
    SCHEMA_REGISTRY_ACCESS_SETTINGS + (SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> HyperdriveSettings.NOTIFICATION_TOPIC)
  }
}
