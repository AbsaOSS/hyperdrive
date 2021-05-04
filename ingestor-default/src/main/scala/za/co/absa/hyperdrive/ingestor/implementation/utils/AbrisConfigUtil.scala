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

import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.commons.configuration2.Configuration
import org.apache.spark.sql.avro.SchemaConverters.toAvroType
import org.apache.spark.sql.catalyst.expressions.Expression
import za.co.absa.abris.avro.read.confluent.SchemaManagerFactory
import za.co.absa.abris.avro.registry.SchemaSubject
import za.co.absa.abris.config.{AbrisConfig, FromAvroConfig, ToAvroConfig}
import za.co.absa.hyperdrive.ingestor.api.utils.ConfigUtils.getOrThrow

private[hyperdrive] object AbrisConfigUtil {
  val TopicNameStrategy = "topic.name"
  val RecordNameStrategy = "record.name"
  val TopicRecordNameStrategy = "topic.record.name"

  def getKeyConsumerSettings(configuration: Configuration, configKeys: AbrisConsumerConfigKeys,
                             schemaRegistryConfig: Map[String, String]): FromAvroConfig =
    getConsumerSettings(configuration, configKeys, isKey = true, schemaRegistryConfig)

  def getValueConsumerSettings(configuration: Configuration, configKeys: AbrisConsumerConfigKeys,
                               schemaRegistryConfig: Map[String, String]): FromAvroConfig =
    getConsumerSettings(configuration, configKeys, isKey = false, schemaRegistryConfig)

  private def getConsumerSettings(configuration: Configuration, configKeys: AbrisConsumerConfigKeys, isKey: Boolean,
                                  schemaRegistryConfig: Map[String, String]): FromAvroConfig = {
    val fromConfluentAvroConfigFragment = AbrisConfig.fromConfluentAvro
    val schemaId = getSchemaId(configuration, configKeys)
    val topic = getTopic(configuration, configKeys)
    val fromSchemaRegisteringConfigFragment = if (schemaId == "latest") {
      val fromStrategyConfigFragment = fromConfluentAvroConfigFragment.downloadReaderSchemaByLatestVersion
      val namingStrategy = getNamingStrategy(configuration, configKeys)
      namingStrategy match {
        case TopicNameStrategy => fromStrategyConfigFragment.andTopicNameStrategy(topic, isKey)
        case RecordNameStrategy => fromStrategyConfigFragment.andRecordNameStrategy(
          getRecordName(configuration, configKeys),
          getRecordNamespace(configuration, configKeys)
        )
        case TopicRecordNameStrategy => fromStrategyConfigFragment.andTopicRecordNameStrategy(topic,
          getRecordName(configuration, configKeys),
          getRecordNamespace(configuration, configKeys)
        )
        case _ => throw new IllegalArgumentException("Naming strategy must be one of topic.name, record.name or topic.record.name")
      }
    } else {
      fromConfluentAvroConfigFragment.downloadReaderSchemaById(schemaId.toInt)
    }

    fromSchemaRegisteringConfigFragment.usingSchemaRegistry(schemaRegistryConfig)
  }

  def getKeyProducerSettings(configuration: Configuration, configKeys: AbrisProducerConfigKeys, schema: Schema,
                             schemaRegistryConfig: Map[String, String]): ToAvroConfig =
    getProducerSettings(configuration, configKeys, isKey = true, schema, schemaRegistryConfig)

  def getValueProducerSettings(configuration: Configuration, configKeys: AbrisProducerConfigKeys, schema: Schema,
                               schemaRegistryConfig: Map[String, String]): ToAvroConfig =
    getProducerSettings(configuration, configKeys, isKey = false, schema, schemaRegistryConfig)

  def generateSchema(configuration: Configuration, configKeys: AbrisProducerConfigKeys, expression: Expression,
                     newDefaultValues: Map[String, Object]): Schema = {
    val namingStrategy = getNamingStrategy(configuration, configKeys)
    val initialSchema = namingStrategy match {
      case TopicNameStrategy => toAvroType(expression.dataType, expression.nullable)
      case x if x == RecordNameStrategy || x == TopicRecordNameStrategy => toAvroType(expression.dataType,
        expression.nullable, getRecordName(configuration, configKeys), getRecordNamespace(configuration, configKeys))
      case _ => throw new IllegalArgumentException("Naming strategy must be one of topic.name, record.name or topic.record.name")
    }

    cloneSchema(initialSchema, newDefaultValues)
  }

  private def cloneSchema(schema: Schema, newDefaultValues: Map[String, Object], fieldPrefix: String = ""): Schema = {
    val prefixSeparator = if (fieldPrefix.isEmpty) "" else "."
    import scala.collection.JavaConverters._
    if (schema.getType != Schema.Type.RECORD) {
      schema
    } else {
      val newFields = schema.getFields.asScala.map(f => {
        val fullFieldName = s"$fieldPrefix$prefixSeparator${f.name()}"
        val defaultValue = newDefaultValues.getOrElse(fullFieldName, f.defaultVal())
        new Schema.Field(f.name(), cloneSchema(f.schema(), newDefaultValues, fullFieldName),
          f.doc(), defaultValue, f.order())
      })
      Schema.createRecord(schema.getName, schema.getDoc, schema.getNamespace, schema.isError, newFields.asJava)
    }
  }


  private def getProducerSettings(configuration: Configuration, configKeys: AbrisProducerConfigKeys, isKey: Boolean,
                                  schema: Schema, schemaRegistryConfig: Map[String, String]): ToAvroConfig = {
    val schemaManager = SchemaManagerFactory.create(schemaRegistryConfig)
    val topic = getTopic(configuration, configKeys)
    val namingStrategy = getNamingStrategy(configuration, configKeys)
    val schemaId = namingStrategy match {
      case TopicNameStrategy =>
        val subject = SchemaSubject.usingTopicNameStrategy(topic, isKey)
        schemaManager.register(subject, schema)
      case RecordNameStrategy =>
        val subject = SchemaSubject.usingRecordNameStrategy(schema)
        schemaManager.register(subject, schema)
      case TopicRecordNameStrategy =>
        val subject = SchemaSubject.usingTopicRecordNameStrategy(topic, schema)
        schemaManager.register(subject, schema)
      case _ => throw new IllegalArgumentException("Naming strategy must be one of topic.name, record.name or topic.record.name")
    }

    AbrisConfig
      .toConfluentAvro
      .downloadSchemaById(schemaId)
      .usingSchemaRegistry(schemaRegistryConfig)
  }

  private def getTopic(configuration: Configuration, configKeys: AbrisConfigKeys): String =
    getOrThrow(configKeys.topic, configuration, errorMessage = s"Topic not found. Is '${configKeys.topic}' properly set?")

  private def getSchemaId(configuration: Configuration, configKeys: AbrisConsumerConfigKeys) =
    getOrThrow(configKeys.schemaId, configuration, errorMessage = s"Schema id not specified. Is '${configKeys.schemaId}' configured?")

  private def getNamingStrategy(configuration: Configuration, configKeys: AbrisConfigKeys) =
    getOrThrow(configKeys.namingStrategy, configuration, errorMessage = s"Schema naming strategy not specified. Is '${configKeys.namingStrategy}' configured?")

  private def getRecordName(configuration: Configuration, configKeys: AbrisConfigKeys) =
    getOrThrow(configKeys.recordName, configuration, errorMessage = s"Record name not specified for value. Is '${configKeys.recordName}' configured?")

  private def getRecordNamespace(configuration: Configuration, configKeys: AbrisConfigKeys) =
    getOrThrow(configKeys.recordNamespace, configuration, errorMessage = s"Record namespace not specified for value. Is '${configKeys.recordNamespace}' configured?")
}
