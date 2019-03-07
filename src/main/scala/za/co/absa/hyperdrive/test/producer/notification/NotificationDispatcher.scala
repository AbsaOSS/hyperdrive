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

package za.co.absa.hyperdrive.test.producer.notification

import org.apache.spark.sql.{DataFrame, SparkSession}
import za.co.absa.abris.avro.read.confluent.SchemaManager
import za.co.absa.hyperdrive.settings.InfrastructureSettings._
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
      .option(KafkaSettings.SPARK_BROKERS_SETTING_KEY, KafkaSettings.BROKERS)
      .option(KafkaSettings.TOPIC_DISPATCH_KEY, HyperdriveSettings.NOTIFICATION_TOPIC)
      .save()
  }

  def getSchemaRegistrySettings: Map[String,String] = {
    SchemaRegistrySettings.SCHEMA_REGISTRY_ACCESS_SETTINGS + (SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> HyperdriveSettings.NOTIFICATION_TOPIC)
  }
}
