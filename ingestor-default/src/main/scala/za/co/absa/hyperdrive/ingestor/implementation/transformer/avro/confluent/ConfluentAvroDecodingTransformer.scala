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
import org.apache.commons.lang3.RandomStringUtils
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame}
import za.co.absa.abris.avro.functions.from_avro
import za.co.absa.abris.config.FromAvroConfig
import za.co.absa.hyperdrive.ingestor.api.context.HyperdriveContext
import za.co.absa.hyperdrive.ingestor.api.transformer.{StreamTransformer, StreamTransformerFactory}
import za.co.absa.hyperdrive.ingestor.api.utils.ConfigUtils
import za.co.absa.hyperdrive.ingestor.implementation.HyperdriveContextKeys
import za.co.absa.hyperdrive.ingestor.implementation.reader.kafka.KafkaStreamReader.KEY_TOPIC
import za.co.absa.hyperdrive.ingestor.implementation.utils.{SchemaRegistryConsumerConfigKeys, AbrisConfigUtil}

private[transformer] class ConfluentAvroDecodingTransformer(
  val valueAvroConfig: FromAvroConfig,
  val keyAvroConfigOpt: Option[FromAvroConfig])
  extends StreamTransformer {

  private val logger = LogManager.getLogger

  override def transform(dataFrame: DataFrame): DataFrame = {
    logger.info(s"SchemaRegistry settings: $valueAvroConfig")

    keyAvroConfigOpt match {
      case Some(keyAvroConfig) => getKeyValueDataFrame(dataFrame, keyAvroConfig)
      case None => getValueDataFrame(dataFrame)
    }
  }

  private def getKeyValueDataFrame(dataFrame: DataFrame, keyAvroConfig: FromAvroConfig) = {
    val decodedDf = dataFrame.select(
      from_avro(col("key"), keyAvroConfig) as 'key,
      from_avro(col("value"), valueAvroConfig) as 'value)
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
      .select(from_avro(col("value"), valueAvroConfig) as 'data)
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

  object SchemaConfigKeys extends SchemaRegistryConsumerConfigKeys {
    override val topic: String = KEY_TOPIC
    override val schemaRegistryUrl: String = KEY_SCHEMA_REGISTRY_URL
    override val schemaId: String = KEY_SCHEMA_REGISTRY_VALUE_SCHEMA_ID
    override val namingStrategy: String = KEY_SCHEMA_REGISTRY_VALUE_NAMING_STRATEGY
    override val recordName: String = KEY_SCHEMA_REGISTRY_VALUE_RECORD_NAME
    override val recordNamespace: String = KEY_SCHEMA_REGISTRY_VALUE_RECORD_NAMESPACE
  }

  override def apply(config: Configuration): StreamTransformer = {
    val valueAvroConfig = AbrisConfigUtil.getValueConsumerSettings(config, SchemaConfigKeys)

    val consumeKeys = ConfigUtils.getOptionalBoolean(KEY_CONSUME_KEYS, config).getOrElse(false)
    val keyAvroConfigOpt = if (consumeKeys) {
      Some(AbrisConfigUtil.getKeyConsumerSettings(config, SchemaConfigKeys))
    } else {
      None
    }
    LogManager.getLogger.info(
      s"Going to create ConfluentAvroDecodingTransformer instance using " +
        s"value avro config='$valueAvroConfig', key avro config='$keyAvroConfigOpt'.")

    new ConfluentAvroDecodingTransformer(valueAvroConfig, keyAvroConfigOpt)
  }

  override def getMappingFromRetainedGlobalConfigToLocalConfig(globalConfig: Configuration): Map[String, String] = Map(
    KEY_TOPIC -> KEY_TOPIC
  )

  def determineKeyColumnPrefix(valueColumnNames: Seq[String]): String = {
    var candidatePrefix = "key__"
    while (valueColumnNames.exists(c => c.startsWith(candidatePrefix))) {
      candidatePrefix = s"${RandomStringUtils.randomAlphanumeric(keyColumnPrefixLength)}_"
    }
    candidatePrefix
  }
}
