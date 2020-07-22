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

package za.co.absa.hyperdrive.ingestor.implementation.utils

import org.apache.commons.configuration2.Configuration
import za.co.absa.abris.avro.read.confluent.SchemaManager
import za.co.absa.abris.avro.read.confluent.SchemaManager.SchemaStorageNamingStrategies
import za.co.absa.hyperdrive.ingestor.api.utils.ConfigUtils.getOrThrow

private[hyperdrive] object SchemaRegistrySettingsUtil {
  def getConsumerSettings(configuration: Configuration, topic: String, configKeys: SchemaRegistryConsumerConfigKeys): Map[String, String] = {
    getCommonSettings(configuration, topic, configKeys) +
      (configKeys.paramSchemaId -> getOrThrow(configKeys.schemaId, configuration,
        errorMessage = s"Schema id not specified. Is '${configKeys.schemaId}' configured?"))
  }

  def getProducerSettings(configuration: Configuration, topic: String, configKeys: SchemaRegistryProducerConfigKeys): Map[String, String] = {
    getCommonSettings(configuration, topic, configKeys)
  }

  private def getCommonSettings(configuration: Configuration, topic: String, configKeys: SchemaRegistryConfigKeys): Map[String, String] = {
    import SchemaManager._
    val settings = Map[String, String](
      PARAM_SCHEMA_REGISTRY_TOPIC -> topic,
      PARAM_SCHEMA_REGISTRY_URL -> getOrThrow(configKeys.schemaRegistryUrl, configuration, errorMessage = s"Schema Registry URL not specified. Is '${configKeys.schemaRegistryUrl}' configured?"),
      configKeys.paramSchemaNamingStrategy -> getOrThrow(configKeys.namingStrategy, configuration, errorMessage = s"Schema naming strategy not specified. Is '${configKeys.namingStrategy}' configured?")
    )

    settings ++ getRecordSettings(settings, configuration, configKeys)
  }

  private def getRecordSettings(currentSettings: Map[String, String], configuration: Configuration, configKeys: SchemaRegistryConfigKeys): Map[String, String] = {
    val namingStrategy = currentSettings(configKeys.paramSchemaNamingStrategy)

    if (SchemaRegistrySettingsUtil.namingStrategyInvolvesRecord(namingStrategy)) {
      Map(
        configKeys.paramSchemaNameForRecordStrategy -> getOrThrow(configKeys.recordName, configuration, errorMessage = s"Record name not specified for value. Is '${configKeys.recordName}' configured?"),
        configKeys.paramSchemaNamespaceForRecordStrategy -> getOrThrow(configKeys.recordNamespace, configuration, errorMessage = s"Record namespace not specified for value. Is '${configKeys.recordNamespace}' configured?")
      )
    } else {
      Map()
    }
  }

  private def namingStrategyInvolvesRecord(strategy: String): Boolean = {
    import SchemaStorageNamingStrategies._
    strategy == RECORD_NAME || strategy == TOPIC_RECORD_NAME
  }
}
