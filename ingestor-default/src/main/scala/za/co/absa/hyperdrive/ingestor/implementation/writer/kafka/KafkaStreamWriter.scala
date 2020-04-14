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

package za.co.absa.hyperdrive.ingestor.implementation.writer.kafka

import org.apache.commons.configuration2.Configuration
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import za.co.absa.abris.avro.read.confluent.SchemaManager
import za.co.absa.abris.avro.read.confluent.SchemaManager.{PARAM_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY, PARAM_SCHEMA_NAME_FOR_RECORD_STRATEGY, PARAM_VALUE_SCHEMA_NAMING_STRATEGY}
import za.co.absa.hyperdrive.ingestor.api.manager.StreamManager
import za.co.absa.hyperdrive.ingestor.api.utils.ComponentFactoryUtil
import za.co.absa.hyperdrive.ingestor.api.writer.{StreamWriter, StreamWriterFactory}
import za.co.absa.hyperdrive.ingestor.implementation.utils.SchemaRegistrySettingsUtil
import za.co.absa.hyperdrive.ingestor.api.utils.ConfigUtils._

private[writer] class KafkaStreamWriter(topic: String,
                                        brokers: String,
                                        schemaRegistrySettings: Map[String, String],
                                        extraOptions: Map[String, String]) extends StreamWriter {

  def write(dataFrame: DataFrame, streamManager: StreamManager): StreamingQuery = {

    import org.apache.spark.sql.functions.struct
    import za.co.absa.abris.avro.functions.to_confluent_avro

    val allColumns = struct(dataFrame.columns.head, dataFrame.columns.tail: _*)

    val dataStreamWriter = dataFrame
      .select(to_confluent_avro(allColumns, schemaRegistrySettings) as 'value)
      .writeStream
    val streamWriterWithOptions = dataStreamWriter.options(extraOptions)
    val configuredStreamWriter = streamManager.configure(streamWriterWithOptions,
      dataFrame.sparkSession.sparkContext.hadoopConfiguration)

    configuredStreamWriter
      .trigger(Trigger.Once())
      .option("topic", topic)
      .option("kafka.bootstrap.servers", brokers)
      .format("kafka")
      .start()
  }
}

object KafkaStreamWriter extends StreamWriterFactory with KafkaStreamWriterAttributes {

  private val logger = LogManager.getLogger

  override def apply(configuration: Configuration): StreamWriter = {
    logger.info(s"Building ${KafkaStreamWriter.getClass.getCanonicalName}")

    ComponentFactoryUtil.validateConfiguration(configuration, getProperties)
    val topic = configuration.getString(KEY_TOPIC)
    val brokers = configuration.getString(KEY_BROKERS)
    val schemaRegistrySettings = getSchemaRegistrySettings(configuration, topic)
    val extraOptions = getPropertySubset(configuration, optionalConfKey)

    logger.info(s"Creating writer: topic = '$topic', brokers = '$brokers', " +
      s"schema registry settings = '$schemaRegistrySettings'")

    new KafkaStreamWriter(topic, brokers, schemaRegistrySettings, extraOptions)
  }

  private def getSchemaRegistrySettings(configuration: Configuration, topic: String): Map[String, String] = {
    import SchemaManager._
    val settings = Map[String, String](
      PARAM_SCHEMA_REGISTRY_TOPIC -> topic,
      PARAM_SCHEMA_REGISTRY_URL -> configuration.getString(KEY_SCHEMA_REGISTRY_URL),
      PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> configuration.getString(KEY_SCHEMA_REGISTRY_VALUE_NAMING_STRATEGY)
    )
    settings ++ getRecordSettings(settings, configuration)
  }

  private def getRecordSettings(currentSettings: Map[String, String], configuration: Configuration): Map[String, String] = {
    val valueNamingStrategy = currentSettings(PARAM_VALUE_SCHEMA_NAMING_STRATEGY)
    if (SchemaRegistrySettingsUtil.namingStrategyInvolvesRecord(valueNamingStrategy)) {
      Map(
        PARAM_SCHEMA_NAME_FOR_RECORD_STRATEGY -> getOrThrow(KEY_SCHEMA_REGISTRY_VALUE_RECORD_NAME, configuration),
        PARAM_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY -> getOrThrow(KEY_SCHEMA_REGISTRY_VALUE_RECORD_NAMESPACE, configuration)
      )
    } else {
      Map()
    }
  }
}
