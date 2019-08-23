/*
 *  Copyright 2019 ABSA Group Limited
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package za.co.absa.hyperdrive.ingestor.implementation.decoder.factories.avro.confluent

import org.apache.commons.configuration2.Configuration
import org.apache.commons.lang3.StringUtils
import org.apache.logging.log4j.LogManager
import za.co.absa.abris.avro.read.confluent.SchemaManager
import za.co.absa.abris.avro.read.confluent.SchemaManager.{PARAM_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY, PARAM_SCHEMA_NAME_FOR_RECORD_STRATEGY, PARAM_VALUE_SCHEMA_NAMING_STRATEGY, SchemaStorageNamingStrategies}
import za.co.absa.abris.avro.schemas.policy.SchemaRetentionPolicies.SchemaRetentionPolicy
import za.co.absa.hyperdrive.ingestor.api.decoder.StreamDecoder
import za.co.absa.hyperdrive.ingestor.implementation.decoder.StreamDecoderFactory
import za.co.absa.hyperdrive.ingestor.implementation.decoder.avro.confluent.ConfluentAvroKafkaStreamDecoder
import za.co.absa.hyperdrive.shared.configurations.ConfigurationsKeys.AvroKafkaStreamDecoderKeys._
import za.co.absa.hyperdrive.shared.utils.ConfigUtils._

private[factories] object ConfluentAvroKafkaStreamDecoderFactory extends StreamDecoderFactory {

  override def name: String = "ConfluentAvroKafkaStreamDecoder"

  override def build(config: Configuration): StreamDecoder = {
    val topic = getTopic(config)
    val schemaRegistrySettings = getSchemaRegistrySettings(config)
    val schemaRetentionPolicy = getSchemaRetentionPolicy(config)

    LogManager.getLogger.info(s"Going to create AvroKafkaStreamDecoder instance using: topic='$topic', schema retention policy='$schemaRetentionPolicy', schema registry settings='$schemaRegistrySettings'.")

    new ConfluentAvroKafkaStreamDecoder(topic, schemaRegistrySettings, schemaRetentionPolicy)
  }

  private def getTopic(configuration: Configuration): String = getOrThrow(KEY_TOPIC, configuration, errorMessage = s"Topic not found. Is '$KEY_TOPIC' properly set?")

  private def getSchemaRegistrySettings(configuration: Configuration): Map[String,String] = {
    import SchemaManager._
    val settings = Map[String,String](
      PARAM_SCHEMA_REGISTRY_URL -> getOrThrow(KEY_SCHEMA_REGISTRY_URL, configuration, errorMessage = s"Schema Registry URL not specified. Is '$KEY_SCHEMA_REGISTRY_URL' configured?"),
      PARAM_VALUE_SCHEMA_ID -> getOrThrow(KEY_SCHEMA_REGISTRY_VALUE_SCHEMA_ID, configuration, errorMessage = s"Schema id not specified for value. Is '$KEY_SCHEMA_REGISTRY_VALUE_SCHEMA_ID' configured?"),
      PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> getOrThrow(KEY_SCHEMA_REGISTRY_VALUE_NAMING_STRATEGY, configuration, errorMessage = s"Schema naming strategy not specified for value. Is '$KEY_SCHEMA_REGISTRY_VALUE_NAMING_STRATEGY' configured?")
    )

    settings ++ getRecordSettings(settings, configuration)
  }

  private def getRecordSettings(currentSettings: Map[String,String], configuration: Configuration): Map[String,String] = {
    val valueNamingStrategy = currentSettings(PARAM_VALUE_SCHEMA_NAMING_STRATEGY)

    if (namingStrategyInvolvesRecord(valueNamingStrategy)) {
      Map(
        PARAM_SCHEMA_NAME_FOR_RECORD_STRATEGY -> getOrThrow(KEY_SCHEMA_REGISTRY_VALUE_RECORD_NAME, configuration, errorMessage = s"Record name not specified for value. Is '$KEY_SCHEMA_REGISTRY_VALUE_RECORD_NAME' configured?"),
        PARAM_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY -> getOrThrow(KEY_SCHEMA_REGISTRY_VALUE_RECORD_NAMESPACE, configuration, errorMessage = s"Record namespace not specified for value. Is '$KEY_SCHEMA_REGISTRY_VALUE_RECORD_NAMESPACE' configured?")
      )
    } else {
      Map()
    }
  }

  private def getSchemaRetentionPolicy(configuration: Configuration): SchemaRetentionPolicy = {
    import za.co.absa.abris.avro.schemas.policy.SchemaRetentionPolicies._
    val policyName = getOrThrow(KEY_SCHEMA_RETENTION_POLICY, configuration, errorMessage = s"Schema retention policy not informed. Is '$KEY_SCHEMA_RETENTION_POLICY' defined?")
    policyName match {
      case "RETAIN_ORIGINAL_SCHEMA"      => RETAIN_ORIGINAL_SCHEMA
      case "RETAIN_SELECTED_COLUMN_ONLY" => RETAIN_SELECTED_COLUMN_ONLY
      case _ => throw new IllegalArgumentException(s"Invalid schema retention policy name: '$policyName'.")
    }
  }

  private def namingStrategyInvolvesRecord(strategy: String): Boolean = {
    import SchemaStorageNamingStrategies._
    strategy == RECORD_NAME || strategy == TOPIC_RECORD_NAME
  }
}
