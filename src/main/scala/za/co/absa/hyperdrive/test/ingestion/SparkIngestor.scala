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

package za.co.absa.hyperdrive.test.ingestion

import org.apache.spark.sql._
import org.apache.spark.sql.streaming.{DataStreamReader, OutputMode}
import za.co.absa.abris.avro.read.confluent.SchemaManager
import za.co.absa.abris.avro.schemas.policy.SchemaRetentionPolicies
import za.co.absa.hyperdrive.test.settings.InfrastructureSettings._

object SparkIngestor {

  def ingest(topic: String, destinatinDirName: String)= {

    println(s"STARTING ingestion from topic '$topic' into destination '$destinatinDirName'")

    val spark = SparkSession.builder().appName(s"SparkIngestor-$topic").master("local[*]").getOrCreate()

    val stream = getConfiguredStreamReader(spark, topic)

    val checkpointLocation = resolveCheckpointLocation(topic)

    println(s"CHECKPOINT location resolved to '$checkpointLocation'")

    val writer = getParquetWriterQuery(stream, destinatinDirName, checkpointLocation)

    writer.processAllAvailable()
    writer.stop()

    println(s"FINISHED ingestion from topic '$topic' into destination '$destinatinDirName'")
  }

  private def getConfiguredStreamReader(sparkSession: SparkSession, topic: String): DataFrame = {

    val schemaRegistrySettings = getSchemaRegistry(topic)

    import za.co.absa.abris.avro.AvroSerDe._

    sparkSession
      .readStream
      .format(KafkaSettings.STREAM_FORMAT_KAFKA_NAME)
      .option(KafkaSettings.TOPIC_SUBSCRIPTION_KEY, topic)
      .option(KafkaSettings.SPARK_BROKERS_SETTING_KEY, KafkaSettings.BROKERS)
      //.option(KafkaSettings.STARTING_OFFSETS_KEY, "earliest")
      .fromConfluentAvro("value", Option.empty, Option(schemaRegistrySettings))(SchemaRetentionPolicies.RETAIN_SELECTED_COLUMN_ONLY)
  }

  private def getParquetWriterQuery(stream: DataFrame, destinationDir: String, checkpointLocation: String) = {

    stream
      .writeStream
        .format("parquet")
      .option(SparkSettings.CHECKPOINT_LOCATION_KEY, checkpointLocation)
      .outputMode(OutputMode.Append())
      .start(destinationDir)

  }

  private def resolveCheckpointLocation(topic: String): String = {
    s"${SparkSettings.CHECKPOINT_BASE_LOCATION}_$topic"
  }

  private def getSchemaRegistry(topic: String): Map[String,String] = {
    // attaching topic and schema id to general Schema Registry settings
    SchemaRegistrySettings.SCHEMA_REGISTRY_ACCESS_SETTINGS + (SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> topic, SchemaManager.PARAM_VALUE_SCHEMA_ID -> "latest")
  }
}
