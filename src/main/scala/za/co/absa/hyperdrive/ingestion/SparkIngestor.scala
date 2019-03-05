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

package za.co.absa.hyperdrive.ingestion

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import za.co.absa.abris.avro.read.confluent.SchemaManager
import za.co.absa.abris.avro.schemas.policy.SchemaRetentionPolicies
import za.co.absa.hyperdrive.settings.InfrastructureSettings.{KafkaSettings, SchemaRegistrySettings, SparkSettings}
import za.co.absa.hyperdrive.utils.FileUtils

object SparkIngestor {

  def ingest(topic: String, destinatinDirName: String): Unit= {

    println(s"STARTING ingestion from topic '$topic' into destination '$destinatinDirName'")

    val spark = SparkSession.builder().appName(s"SparkIngestor-$topic").master("local[*]").getOrCreate()

    val checkpointLocation = resolveCheckpointLocation(topic)

    println(s"CHECKPOINT location resolved to '$checkpointLocation'")

    val startingOffsets = getStartingOffsets(checkpointLocation, spark.sparkContext.hadoopConfiguration)

    println(s"STARTING OFFSETS resolved to '$startingOffsets'")

    val stream = getConfiguredStreamReader(spark, topic, startingOffsets)

    val writer = getParquetWriterQuery(stream, destinatinDirName, checkpointLocation)

    writer.processAllAvailable()
    writer.stop()

    println(s"FINISHED ingestion from topic '$topic' into destination '$destinatinDirName'")

    spark.close()
  }

  private def getConfiguredStreamReader(sparkSession: SparkSession, topic: String, startingOffsets: Option[String]): DataFrame = {

    val schemaRegistrySettings = getSchemaRegistry(topic)

    import za.co.absa.abris.avro.AvroSerDe._

    val payload = sparkSession
      .readStream
      .format(KafkaSettings.STREAM_FORMAT_KAFKA_NAME)
      .option(KafkaSettings.TOPIC_SUBSCRIPTION_KEY, topic)
      .option(KafkaSettings.SPARK_BROKERS_SETTING_KEY, KafkaSettings.BROKERS)

      val payloadWithOffset = if (startingOffsets.isDefined)
        payload.option(KafkaSettings.STARTING_OFFSETS_KEY, startingOffsets.get)
      else payload

      payloadWithOffset.fromConfluentAvro("value", Option.empty, Option(schemaRegistrySettings))(SchemaRetentionPolicies.RETAIN_SELECTED_COLUMN_ONLY)
  }

  private def getParquetWriterQuery(stream: DataFrame, destinationDir: String, checkpointLocation: String) = {

    stream
      .writeStream
        .trigger(Trigger.Once)
        .format("parquet")
      .option(SparkSettings.CHECKPOINT_LOCATION_KEY, checkpointLocation)
      .outputMode(OutputMode.Append())
      .start(destinationDir)

  }

  private def resolveCheckpointLocation(topic: String): String = {
    s"${SparkSettings.CHECKPOINT_BASE_LOCATION}/$topic"
  }

  private def getSchemaRegistry(topic: String): Map[String,String] = {
    // attaching topic and schema id to general Schema Registry settings
    SchemaRegistrySettings.SCHEMA_REGISTRY_ACCESS_SETTINGS + (SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> topic, SchemaManager.PARAM_VALUE_SCHEMA_ID -> "latest")
  }

  private def getStartingOffsets(checkpointLocation: String, configuration: Configuration): Option[String] = {
   if (FileUtils.exists(checkpointLocation, configuration)) {
     Option.empty
   }
   else {
     Option(KafkaSettings.STARTING_OFFSETS_EARLIEST)
   }
  }
}
