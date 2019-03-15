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

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{Encoder, Row, SparkSession}
import za.co.absa.abris.avro.format.SparkAvroConversions
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.avro.read.confluent.SchemaManager
import za.co.absa.abris.examples.data.generation.ComplexRecordsGenerator
import za.co.absa.hyperdrive.shared.InfrastructureSettings.{AvroSettings, HyperdriveSettings, KafkaSettings, SchemaRegistrySettings}

object RandomPayloadProducer {

  private val NUM_RECORDS = 5
  HyperdriveSettings.PAYLOAD_TOPIC_IN_USE = HyperdriveSettings.PAYLOAD_TOPIC_1

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("RandomPayloadProducer").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("info")

    implicit val encoder: Encoder[Row] = getEncoder

    val rows = createRows(NUM_RECORDS)

    import spark.implicits._
    val dataframe = spark.sparkContext.parallelize(rows, 2).toDF()

    println("GOING to send payload:")
    dataframe.show()

    import za.co.absa.abris.avro.AvroSerDe._

    dataframe
      .toConfluentAvro(HyperdriveSettings.PAYLOAD_TOPIC_IN_USE, AvroSettings.GENERAL_SCHEMA_NAME, AvroSettings.GENERAL_SCHEMA_NAMESPACE)(SchemaRegistrySettings.SCHEMA_REGISTRY_ACCESS_SETTINGS)
      .write
      .format(KafkaSettings.STREAM_FORMAT_KAFKA_NAME)
      .option(KafkaSettings.SPARK_BROKERS_SETTING_KEY, KafkaSettings.BROKERS)
      .option(KafkaSettings.TOPIC_DISPATCH_KEY, HyperdriveSettings.PAYLOAD_TOPIC_IN_USE)
      .save()

    //spark.close()
    println("PAYLOAD SENT. GOING to notify ingestors")
    NotificationDispatcher.dispatchNotification()
  }

  private def createRows(howMany: Int): List[Row] = {
    ComplexRecordsGenerator.generateUnparsedRows(howMany)
  }

  private def getEncoder: Encoder[Row] = {
    val avroSchema = AvroSchemaUtils.parse(ComplexRecordsGenerator.usedAvroSchema)
    val sparkSchema = SparkAvroConversions.toSqlType(avroSchema)
    println(sparkSchema)
    RowEncoder.apply(sparkSchema)
  }
}
