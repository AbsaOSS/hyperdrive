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

package za.co.absa.hyperdrive.ingestor.implementation.transformer.avro.confluent

import org.apache.commons.configuration2.Configuration
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, struct}
import za.co.absa.abris.avro.functions.to_confluent_avro
import za.co.absa.abris.avro.read.confluent.SchemaManager.{PARAM_KEY_SCHEMA_ID, PARAM_KEY_SCHEMA_NAMING_STRATEGY, PARAM_VALUE_SCHEMA_ID, PARAM_VALUE_SCHEMA_NAMING_STRATEGY}
import za.co.absa.hyperdrive.ingestor.api.context.HyperdriveContext
import za.co.absa.hyperdrive.ingestor.api.transformer.{StreamTransformer, StreamTransformerFactory}
import za.co.absa.hyperdrive.ingestor.api.utils.ConfigUtils
import za.co.absa.hyperdrive.ingestor.implementation.HyperdriveContextKeys
import za.co.absa.hyperdrive.ingestor.implementation.utils.{SchemaRegistryProducerConfigKeys, SchemaRegistrySettingsUtil}
import za.co.absa.hyperdrive.ingestor.implementation.writer.kafka.KafkaStreamWriter.KEY_TOPIC

private[transformer] class ConfluentAvroEncodingTransformer(
  val valueSchemaRegistrySettings: Map[String, String],
  val keySchemaRegistrySettings: Option[Map[String, String]])
  extends StreamTransformer {

  if (valueSchemaRegistrySettings.isEmpty) {
    throw new IllegalArgumentException(
      "Empty Schema Registry settings received.")
  }

  private val logger = LogManager.getLogger

  override def transform(dataFrame: DataFrame): DataFrame = {
    logger.info(s"SchemaRegistry settings: $valueSchemaRegistrySettings")

    keySchemaRegistrySettings match {
      case Some(keySettings) => getKeyValueDataFrame(dataFrame, keySettings)
      case None => getValueDataFrame(dataFrame)
    }
  }

  private def getKeyValueDataFrame(dataFrame: DataFrame, keySchemaRegistrySettings: Map[String, String]): DataFrame = {
    val keyColumnPrefix = HyperdriveContext.get[String](HyperdriveContextKeys.keyColumnPrefix).get
    val keyColumnNames = HyperdriveContext.get[Seq[String]](HyperdriveContextKeys.keyColumnNames).get
    val prefixedKeyColumnNames = keyColumnNames.map(c => s"$keyColumnPrefix$c")

    val valueColumns = dataFrame.columns.toSeq
      .filterNot(columnName => prefixedKeyColumnNames.contains(columnName))
      .map(c => dataFrame(c))
    val unprefixedKeyColumns = keyColumnNames.map(c => dataFrame(s"$keyColumnPrefix$c").as(c))
    val unprefixedDataFrame = dataFrame.select(struct(unprefixedKeyColumns: _*) as 'key, struct(valueColumns: _*) as 'value)
    unprefixedDataFrame.select(
      to_confluent_avro(col("key"), keySchemaRegistrySettings) as 'key,
      to_confluent_avro(col("value"), valueSchemaRegistrySettings) as 'value)
  }

  private def getValueDataFrame(dataFrame: DataFrame): DataFrame = {
    val allColumns = struct(dataFrame.columns.head, dataFrame.columns.tail: _*)
    dataFrame.select(to_confluent_avro(allColumns, valueSchemaRegistrySettings) as 'value)
  }
}

object ConfluentAvroEncodingTransformer extends StreamTransformerFactory with ConfluentAvroEncodingTransformerAttributes {

  object ValueSchemaConfigKeys extends SchemaRegistryProducerConfigKeys {
    override val schemaRegistryUrl: String = KEY_SCHEMA_REGISTRY_URL
    override val namingStrategy: String = KEY_SCHEMA_REGISTRY_VALUE_NAMING_STRATEGY
    override val recordName: String = KEY_SCHEMA_REGISTRY_VALUE_RECORD_NAME
    override val recordNamespace: String = KEY_SCHEMA_REGISTRY_VALUE_RECORD_NAMESPACE
    override val paramSchemaId: String = PARAM_VALUE_SCHEMA_ID
    override val paramSchemaNamingStrategy: String = PARAM_VALUE_SCHEMA_NAMING_STRATEGY
  }

  object KeySchemaConfigKeys extends SchemaRegistryProducerConfigKeys {
    override val schemaRegistryUrl: String = KEY_SCHEMA_REGISTRY_URL
    override val namingStrategy: String = KEY_SCHEMA_REGISTRY_KEY_NAMING_STRATEGY
    override val recordName: String = KEY_SCHEMA_REGISTRY_KEY_RECORD_NAME
    override val recordNamespace: String = KEY_SCHEMA_REGISTRY_KEY_RECORD_NAMESPACE
    override val paramSchemaId: String = PARAM_KEY_SCHEMA_ID
    override val paramSchemaNamingStrategy: String = PARAM_KEY_SCHEMA_NAMING_STRATEGY
  }

  override def apply(config: Configuration): StreamTransformer = {
    val topic = config.getString(KEY_TOPIC)

    val valueSchemaRegistrySettings = SchemaRegistrySettingsUtil.getProducerSettings(config, topic, ValueSchemaConfigKeys)
    val keySchemaRegistrySettingsOpt = ConfigUtils.getOrNone(KEY_PRODUCE_KEYS, config)
      .flatMap(_ => Some(SchemaRegistrySettingsUtil.getProducerSettings(config, topic, KeySchemaConfigKeys)))

    new ConfluentAvroEncodingTransformer(valueSchemaRegistrySettings, keySchemaRegistrySettingsOpt)
  }

  override def getMappingFromRetainedGlobalConfigToLocalConfig(globalConfig: Configuration): Map[String, String] = Map(
    KEY_TOPIC -> KEY_TOPIC
  )

}



