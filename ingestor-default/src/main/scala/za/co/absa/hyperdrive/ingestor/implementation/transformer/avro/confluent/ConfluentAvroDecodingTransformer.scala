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
import org.apache.commons.lang3.{RandomStringUtils, StringUtils}
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame}
import za.co.absa.abris.avro.functions.from_confluent_avro
import za.co.absa.abris.avro.read.confluent.SchemaManager._
import za.co.absa.hyperdrive.ingestor.api.context.HyperdriveContext
import za.co.absa.hyperdrive.ingestor.api.transformer.{StreamTransformer, StreamTransformerFactory}
import za.co.absa.hyperdrive.ingestor.api.utils.ConfigUtils
import za.co.absa.hyperdrive.ingestor.api.utils.ConfigUtils.getOrThrow
import za.co.absa.hyperdrive.ingestor.implementation.HyperdriveContextKeys
import za.co.absa.hyperdrive.ingestor.implementation.utils.{SchemaRegistryConsumerConfigKeys, SchemaRegistrySettingsUtil}
import za.co.absa.hyperdrive.shared.configurations.ConfigurationsKeys.AvroKafkaStreamDecoderKeys._

private[transformer] class ConfluentAvroDecodingTransformer(
  val topic: String,
  val valueSchemaRegistrySettings: Map[String, String],
  val keySchemaRegistrySettings: Option[Map[String, String]])
  extends StreamTransformer {

  if (StringUtils.isBlank(topic)) {
    throw new IllegalArgumentException("Blank topic.")
  }

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

  private def getKeyValueDataFrame(dataFrame: DataFrame,
                                   keySchemaRegistrySettings: Map[String, String]) = {
    val decodedDf = dataFrame.select(
      from_confluent_avro(col("key"), keySchemaRegistrySettings) as 'key,
      from_confluent_avro(col("value"), valueSchemaRegistrySettings) as 'value)
    val keyValueDf = setColumnNonNullable(decodedDf, "value")

    val keyColumnNames = keyValueDf.select("key.*").columns.toSeq
    val valueColumnNames = keyValueDf.select("value.*").columns.toSeq
    val prefix = ConfluentAvroDecodingTransformer.determineKeyColumnPrefix(valueColumnNames)

    HyperdriveContext.put(HyperdriveContextKeys.keyColumnNames, keyColumnNames)
    HyperdriveContext.put(HyperdriveContextKeys.keyColumnPrefix, prefix)

    val prefixedKeyColumns = keyColumnNames.map(c => keyValueDf(s"key.$c").as(s"$prefix$c"))
    val valueColumns = valueColumnNames.map(c => keyValueDf(s"value.$c"))
    keyValueDf.select(prefixedKeyColumns ++ valueColumns: _*)
  }

  private def getValueDataFrame(dataFrame: DataFrame) = {
    val decodedDf = dataFrame
      .select(from_confluent_avro(col("value"), valueSchemaRegistrySettings) as 'data)
    setColumnNonNullable(decodedDf, "data")
      .select("data.*")
  }

  private def setColumnNonNullable(dataFrame: DataFrame, columnName: String) = {
    dataFrame
      .filter(col(columnName).isNotNull)
      .withColumn(columnName, new Column(AssertNotNull(col(columnName).expr)))
  }

}

object ConfluentAvroDecodingTransformer extends StreamTransformerFactory with ConfluentAvroDecodingTransformerAttributes {
  private val keyColumnPrefixLength = 4

  object ValueSchemaConfigKeys extends SchemaRegistryConsumerConfigKeys {
    override val schemaRegistryUrl: String = KEY_SCHEMA_REGISTRY_URL
    override val schemaId: String = KEY_SCHEMA_REGISTRY_VALUE_SCHEMA_ID
    override val namingStrategy: String = KEY_SCHEMA_REGISTRY_VALUE_NAMING_STRATEGY
    override val recordName: String = KEY_SCHEMA_REGISTRY_VALUE_RECORD_NAME
    override val recordNamespace: String = KEY_SCHEMA_REGISTRY_VALUE_RECORD_NAMESPACE
    override val paramSchemaId: String = PARAM_VALUE_SCHEMA_ID
    override val paramSchemaNamingStrategy: String = PARAM_VALUE_SCHEMA_NAMING_STRATEGY
  }

  object KeySchemaConfigKeys extends SchemaRegistryConsumerConfigKeys {
    override val schemaRegistryUrl: String = KEY_SCHEMA_REGISTRY_URL
    override val schemaId: String = KEY_SCHEMA_REGISTRY_KEY_SCHEMA_ID
    override val namingStrategy: String = KEY_SCHEMA_REGISTRY_KEY_NAMING_STRATEGY
    override val recordName: String = KEY_SCHEMA_REGISTRY_KEY_RECORD_NAME
    override val recordNamespace: String = KEY_SCHEMA_REGISTRY_KEY_RECORD_NAMESPACE
    override val paramSchemaId: String = PARAM_KEY_SCHEMA_ID
    override val paramSchemaNamingStrategy: String = PARAM_KEY_SCHEMA_NAMING_STRATEGY
  }

  override def apply(config: Configuration): StreamTransformer = {
    val topic = getTopic(config)
    val valueSchemaRegistrySettings = SchemaRegistrySettingsUtil.getConsumerSettings(config, topic, ValueSchemaConfigKeys)

    val keySchemaRegistrySettingsOpt = ConfigUtils.getOrNone(KEY_CONSUME_KEYS, config)
      .flatMap(_ => Some(SchemaRegistrySettingsUtil.getConsumerSettings(config, topic, KeySchemaConfigKeys)))
    LogManager.getLogger.info(
      s"Going to create ConfluentAvroDecodingTransformer instance using: topic='$topic', " +
        s"value schema registry settings='$valueSchemaRegistrySettings', key schema registry settings='$keySchemaRegistrySettingsOpt'.")

    new ConfluentAvroDecodingTransformer(topic, valueSchemaRegistrySettings, keySchemaRegistrySettingsOpt)
  }

  private def getTopic(configuration: Configuration): String =
    getOrThrow(KEY_TOPIC, configuration, errorMessage = s"Topic not found. Is '$KEY_TOPIC' properly set?")

  def determineKeyColumnPrefix(valueColumnNames: Seq[String]): String = {
    var candidatePrefix = "key__"
    while (valueColumnNames.exists(c => c.startsWith(candidatePrefix))) {
      candidatePrefix = s"${RandomStringUtils.randomAlphanumeric(keyColumnPrefixLength)}_"
    }
    candidatePrefix
  }
}
