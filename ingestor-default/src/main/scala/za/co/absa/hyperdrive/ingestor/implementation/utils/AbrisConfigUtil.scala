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

import org.apache.avro.{JsonProperties, Schema}
import org.apache.commons.configuration2.Configuration
import org.apache.spark.sql.catalyst.expressions.Expression
import za.co.absa.abris.avro.read.confluent.SchemaManagerFactory
import za.co.absa.abris.avro.registry.SchemaSubject
import za.co.absa.abris.config.{AbrisConfig, FromAvroConfig, ToAvroConfig}
import za.co.absa.hyperdrive.ingestor.api.utils.ConfigUtils
import za.co.absa.hyperdrive.ingestor.api.utils.ConfigUtils.getOrThrow
import za.co.absa.hyperdrive.ingestor.implementation.transformer.avro.confluent.{AdvancedAvroToSparkConverter, DefaultSparkToAvroConverter, SparkToAvroConverter}

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

    val fromAvroConfig = fromSchemaRegisteringConfigFragment.usingSchemaRegistry(schemaRegistryConfig)
    if (getUseAdvancedSchemaConversion(configuration, configKeys)) {
      fromAvroConfig.withSchemaConverter(AdvancedAvroToSparkConverter.name)
    } else {
      fromAvroConfig
    }
  }

  def getKeyProducerSettings(configuration: Configuration, configKeys: AbrisProducerConfigKeys, schema: Schema,
                             schemaRegistryConfig: Map[String, String]): ToAvroConfig =
    getProducerSettings(configuration, configKeys, isKey = true, schema, schemaRegistryConfig)

  def getValueProducerSettings(configuration: Configuration, configKeys: AbrisProducerConfigKeys, schema: Schema,
                               schemaRegistryConfig: Map[String, String]): ToAvroConfig =
    getProducerSettings(configuration, configKeys, isKey = false, schema, schemaRegistryConfig)

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

  /**
   * Generates an avro schema given a Spark expression. Record name and namespace are derived according to the
   * configured naming strategy. Default values for the avro schema can be passed using a key-value map. The keys
   * need to correspond to the field names. In case of nested structs, nested field names should be concatenated
   * using the dot (.), e.g. "parent.childField.subChildField". Note that dots in avro field names are not allowed.
   */
  def generateSchema(configuration: Configuration, configKeys: AbrisProducerConfigKeys, expression: Expression,
                     newDefaultValues: Map[String, Object],
                     sparkToAvroConverter: SparkToAvroConverter = DefaultSparkToAvroConverter): Schema = {
    val namingStrategy = getNamingStrategy(configuration, configKeys)
    val initialSchema = namingStrategy match {
      case TopicNameStrategy => sparkToAvroConverter(expression.dataType, expression.nullable)
      case x if x == RecordNameStrategy || x == TopicRecordNameStrategy => sparkToAvroConverter(expression.dataType,
        expression.nullable, getRecordName(configuration, configKeys), getRecordNamespace(configuration, configKeys))
      case _ => throw new IllegalArgumentException("Naming strategy must be one of topic.name, record.name or topic.record.name")
    }

    updateSchema(initialSchema, newDefaultValues)
  }

  /**
   * This method is intended to update schemas created by [[org.apache.spark.sql.avro.SchemaConverters.toAvroType]] with
   * new default values.
   * Apart from the basic types, it only supports the complex types Record, Map and Array. New default values for Enum
   * or Fixed cannot be assigned. Updating default values for the union type is only supported for a union with null.
   * The correct order of arbitrary unions with respect to the given default value is not guaranteed.
   */
  private def updateSchema(schema: Schema, newDefaultValues: Map[String, Object], fieldPrefix: String = ""): Schema = {
    val prefixSeparator = if (fieldPrefix.isEmpty) "" else "."
    import scala.collection.JavaConverters._
    schema.getType match {
      case Schema.Type.UNION =>
        val newSchemas = schema.getTypes.asScala.map(t =>
          updateSchema(t, newDefaultValues, fieldPrefix)
        )
        Schema.createUnion(newSchemas.asJava)
      case Schema.Type.RECORD =>
        val newFields = schema.getFields.asScala.map(f => {
          val fullFieldName = s"$fieldPrefix$prefixSeparator${f.name()}"
          val defaultValue = newDefaultValues.getOrElse(fullFieldName, f.defaultVal())
          val newSchema = updateSchema(f.schema(), newDefaultValues, fullFieldName)
          val newSchemaReordered = reorderUnionTypesForDefaultValueNull(newSchema, defaultValue)
          new Schema.Field(f.name(), newSchemaReordered, f.doc(), defaultValue, f.order())
        })
        Schema.createRecord(schema.getName, schema.getDoc, schema.getNamespace, schema.isError, newFields.asJava)
      case Schema.Type.ARRAY =>
        val newSchema = updateSchema(schema.getElementType, newDefaultValues, fieldPrefix)
        Schema.createArray(newSchema)
      case Schema.Type.MAP =>
        val newSchema = updateSchema(schema.getValueType, newDefaultValues, fieldPrefix)
        Schema.createMap(newSchema)
      case _ => schema
    }
  }

  private def reorderUnionTypesForDefaultValueNull(schema: Schema, defaultValue: Object) = {
    import scala.collection.JavaConverters._
    lazy val schemaTypes = schema.getTypes.asScala
    if (schema.getType == Schema.Type.UNION &&
      schemaTypes.size == 2 &&
      schemaTypes.head.getType != Schema.Type.NULL &&
      schemaTypes(1).getType == Schema.Type.NULL &&
      defaultValue.isInstanceOf[JsonProperties.Null]
    ) {
      Schema.createUnion(Schema.create(Schema.Type.NULL), schemaTypes.head)
    } else {
      schema
    }
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

  private def getUseAdvancedSchemaConversion(configuration: Configuration, configKeys: AbrisConsumerConfigKeys) = {
    ConfigUtils.getOptionalBoolean(configKeys.useAdvancedSchemaConversion, configuration).getOrElse(false)
  }
}
