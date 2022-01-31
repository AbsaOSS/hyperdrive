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

import org.apache.avro.JsonProperties
import org.apache.commons.configuration2.Configuration
import org.slf4j.LoggerFactory
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.functions.struct
import za.co.absa.abris.avro.functions.to_avro
import za.co.absa.abris.config.ToAvroConfig
import za.co.absa.hyperdrive.ingestor.api.context.HyperdriveContext
import za.co.absa.hyperdrive.ingestor.api.transformer.{StreamTransformer, StreamTransformerFactory}
import za.co.absa.hyperdrive.ingestor.api.utils.ConfigUtils
import za.co.absa.hyperdrive.ingestor.implementation.HyperdriveContextKeys
import za.co.absa.hyperdrive.ingestor.implementation.transformer.avro.confluent.ConfluentAvroEncodingTransformer.{getKeyAvroConfig, getValueAvroConfig}
import za.co.absa.hyperdrive.ingestor.implementation.utils.{AbrisConfigUtil, AbrisProducerConfigKeys, SchemaRegistryConfigUtil}
import za.co.absa.hyperdrive.ingestor.implementation.writer.kafka.KafkaStreamWriter.KEY_TOPIC

private[transformer] class ConfluentAvroEncodingTransformer(
  val config: Configuration,
  val withKey: Boolean)
  extends StreamTransformer {

  private val logger = LoggerFactory.getLogger(this.getClass)

  override def transform(dataFrame: DataFrame): DataFrame = {
    if (withKey) {
      getKeyValueDataFrame(dataFrame)
    } else {
      getValueDataFrame(dataFrame)
    }
  }

  private def getKeyValueDataFrame(dataFrame: DataFrame): DataFrame = {
    val keyColumnPrefix = HyperdriveContext.get[String](HyperdriveContextKeys.keyColumnPrefix).get
    val keyColumnNames = HyperdriveContext.get[Seq[String]](HyperdriveContextKeys.keyColumnNames).get
    val prefixedKeyColumnNames = keyColumnNames.map(c => s"$keyColumnPrefix$c")

    val valueColumns = dataFrame.columns.toSeq
      .filterNot(columnName => prefixedKeyColumnNames.contains(columnName))
      .map(c => dataFrame(c))
    val unprefixedKeyColumns = keyColumnNames.map(c => dataFrame(s"$keyColumnPrefix$c").as(c))
    val keyStruct = struct(unprefixedKeyColumns: _*) as 'key
    val valueStruct = struct(valueColumns: _*) as 'value
    val keyToAvroConfig = getKeyAvroConfig(config, keyStruct.expr)
    val valueToAvroConfig = getValueAvroConfig(config, valueStruct.expr)
    logger.info(s"Key ToAvro settings: $keyToAvroConfig")
    logger.info(s"Value ToAvro settings: $valueToAvroConfig")
    dataFrame.select(
      to_avro(keyStruct, keyToAvroConfig) as 'key,
      to_avro(valueStruct, valueToAvroConfig) as 'value)
  }

  private def getValueDataFrame(dataFrame: DataFrame): DataFrame = {
    val allColumns = struct(dataFrame.columns.map(c => dataFrame(c)): _*)
    val toAvroConfig = getValueAvroConfig(config, allColumns.expr)
    logger.info(s"ToAvro settings: $toAvroConfig")
    dataFrame.select(to_avro(allColumns, toAvroConfig) as 'value)
  }
}

object ConfluentAvroEncodingTransformer extends StreamTransformerFactory with ConfluentAvroEncodingTransformerAttributes {
  private val logger = LoggerFactory.getLogger(this.getClass)

  object AbrisConfigKeys extends AbrisProducerConfigKeys {
    override val topic: String = KEY_TOPIC
    override val namingStrategy: String = KEY_SCHEMA_REGISTRY_VALUE_NAMING_STRATEGY
    override val recordName: String = KEY_SCHEMA_REGISTRY_VALUE_RECORD_NAME
    override val recordNamespace: String = KEY_SCHEMA_REGISTRY_VALUE_RECORD_NAMESPACE
  }

  override def apply(config: Configuration): StreamTransformer = {
    val withKey = ConfigUtils.getOptionalBoolean(KEY_PRODUCE_KEYS, config).getOrElse(false)
    new ConfluentAvroEncodingTransformer(config, withKey)
  }

  override def getMappingFromRetainedGlobalConfigToLocalConfig(globalConfig: Configuration): Map[String, String] = Map(
    KEY_TOPIC -> KEY_TOPIC
  )

  def getKeyAvroConfig(config: Configuration, expression: Expression): ToAvroConfig = {
    val schemaRegistryConfig = SchemaRegistryConfigUtil.getSchemaRegistryConfig(config)
    val newDefaultValues = ConfigUtils.getSeqOrNone(KEY_KEY_OPTIONAL_FIELDS, config)
      .map(optionalFields => optionalFields.map(_ -> JsonProperties.NULL_VALUE).toMap)
      .getOrElse(Map())

    val schema = AbrisConfigUtil.generateSchema(config, AbrisConfigKeys, expression, newDefaultValues,
      getSparkToAvroConverter(config))
    logger.info(s"Generated key schema\n${schema.toString(true)}")
    AbrisConfigUtil.getKeyProducerSettings(config, AbrisConfigKeys, schema, schemaRegistryConfig)
  }

  def getValueAvroConfig(config: Configuration, expression: Expression): ToAvroConfig = {
    val schemaRegistryConfig = SchemaRegistryConfigUtil.getSchemaRegistryConfig(config)
    val newDefaultValues = ConfigUtils.getSeqOrNone(KEY_VALUE_OPTIONAL_FIELDS, config)
      .map(optionalFields => optionalFields.map(_ -> JsonProperties.NULL_VALUE).toMap)
      .getOrElse(Map())
    val schema = AbrisConfigUtil.generateSchema(config, AbrisConfigKeys, expression, newDefaultValues,
      getSparkToAvroConverter(config))
    logger.info(s"Generated value schema\n${schema.toString(true)}")
    AbrisConfigUtil.getValueProducerSettings(config, AbrisConfigKeys, schema, schemaRegistryConfig)
  }

  private def getSparkToAvroConverter(config: Configuration) =
    ConfigUtils.getOptionalBoolean(KEY_USE_ADVANCED_SCHEMA_CONVERSION, config)
      .filter(x => x)
      .map(_ => AdvancedSparkToAvroConverter)
      .getOrElse(DefaultSparkToAvroConverter)
}



