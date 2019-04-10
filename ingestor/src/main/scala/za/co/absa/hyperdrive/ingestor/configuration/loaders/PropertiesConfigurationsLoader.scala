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

package za.co.absa.hyperdrive.ingestor.configuration.loaders

import java.io.{FileInputStream, InputStream}
import java.util.Properties

import org.apache.logging.log4j.LogManager
import za.co.absa.abris.avro.schemas.policy.SchemaRetentionPolicies.SchemaRetentionPolicy
import za.co.absa.hyperdrive.ingestor.configuration.CompositeIngestionConfigurations
import za.co.absa.hyperdrive.ingestor.configuration.components._

import scala.util.{Failure, Success, Try}

object SparkConfKeys {
  val KEY_SPARK_APP_NAME = "spark.app.name"
}

object StreamReaderKeys {
  val KEY_EXTRA_CONFS: String = "source.extra.confs"
  val KEY_SOURCE_TOPIC: String = "source.topic"
  val KEY_SOURCE_BROKERS: String = "source.brokers"
}

object OffsetManagerKeys {
  val KEY_OFFSET_MANAGER_TOPIC: String = StreamReaderKeys.KEY_SOURCE_TOPIC
  val KEY_CHECKPOINT_BASE_LOCATION: String = "checkpoint.base.location"
}

object StreamDecoderKeys {
  val KEY_TOPIC:String = StreamReaderKeys.KEY_SOURCE_TOPIC
  val KEY_SCHEMA_RETENTION_POLICY: String = "schema.retention.policy"
  val KEY_SCHEMA_REGISTRY_SETTINGS: String = "schema.registry.settings"
}

object StreamWriterKeys {
  val KEY_EXTRA_CONFS: String = "sink.extra.confs"
  val KEY_DESTINATION_DIRECTORY: String = "destination.directory"
}

object StreamTransformerKeys {
  val KEY_TRANSFORMATION_QUERY: String = "stream.tranformation.query"
}

/**
  * This loader loads configurations from a properties file.
  */
private[configuration] object PropertiesConfigurationsLoader {

  private val logger = LogManager.getLogger

  def load(path: String): CompositeIngestionConfigurations = {
    val properties = loadProperties(path)
    CompositeIngestionConfigurations(
      loadSparkConf(properties),
      loadStreamReaderConfs(properties),
      loadOffsetManagerConfs(properties),
      loadStreamDecoderConfs(properties),
      loadStreamWriterConfs(properties),
      loadStreamTransformerConfs(properties)
    )
  }

  private def loadSparkConf(properties: Properties): SparkConf = {
    SparkConf(getOrThrow(SparkConfKeys.KEY_SPARK_APP_NAME, properties))
  }

  private def loadStreamReaderConfs(properties: Properties): KafkaStreamReaderConf = {
    import StreamReaderKeys._
    val topic = getOrThrow(KEY_SOURCE_TOPIC, properties)
    val brokers = getOrThrow(KEY_SOURCE_BROKERS, properties)
    val extraConfs: Map[String,String] = getOrNone(KEY_EXTRA_CONFS, properties) match {
      case Some(confs) => confsToMap(confs)
      case None        => Map[String,String]()
    }

    KafkaStreamReaderConf(topic, brokers, extraConfs)
  }

  private def loadOffsetManagerConfs(properties: Properties): CheckpointingOffsetManagerConf = {
    import OffsetManagerKeys._
    val topic = getOrThrow(KEY_OFFSET_MANAGER_TOPIC, properties)
    val checkpointLocation = getOrThrow(KEY_CHECKPOINT_BASE_LOCATION, properties)

    CheckpointingOffsetManagerConf(topic, checkpointLocation)
  }

  private def loadStreamDecoderConfs(properties: Properties): AvroStreamDecoderConf = {
    import StreamDecoderKeys._
    val topic = getOrThrow(KEY_TOPIC, properties)
    val schemaRegistrySettings = confsToMap(getOrThrow(KEY_SCHEMA_REGISTRY_SETTINGS, properties))
    val schemaRetentionPolicy = stringToRetentionPolicy(getOrThrow(KEY_SCHEMA_RETENTION_POLICY, properties))

    AvroStreamDecoderConf(topic, schemaRegistrySettings, schemaRetentionPolicy)
  }

  private def loadStreamWriterConfs(properties: Properties): ParquetStreamWriterConf = {
    import StreamWriterKeys._
    val destinationDirectory = getOrThrow(KEY_DESTINATION_DIRECTORY, properties)
    val extraConfs = getOrNone(KEY_EXTRA_CONFS, properties) match {
      case Some(confs) => Some(confsToMap(confs))
      case None        => None
    }

    ParquetStreamWriterConf(destinationDirectory, extraConfs)
  }

  private def loadStreamTransformerConfs(properties: Properties): ColumnSelectorStreamTransformerConf = {
    import StreamTransformerKeys._
    val query = getOrThrow(KEY_TRANSFORMATION_QUERY, properties)

    ColumnSelectorStreamTransformerConf(query.split(","))
  }

  private def loadProperties(path: String): Properties = {
    Try({
      val properties = new Properties()
      val inputStream = new FileInputStream(path)
      properties.load(inputStream)
      inputStream.close()
      properties
    }) match {
      case Success(properties) => properties
      case Failure(exception)  => throw new IllegalArgumentException(s"Could not load properties from '$path'. Is it a valid properties file?", exception)
    }
  }

  private def getOrThrow(key: String, properties: Properties): String = {
    if (!properties.containsKey(key)) {
      throw new IllegalArgumentException(s"Property not found: '$key'.")
    }
    properties.getProperty(key)
  }

  private def getOrNone(key: String, properties: Properties): Option[String] = {
    Try(getOrThrow(key, properties)) match {
      case Success(value) => Some(value)
      case Failure(ex)    => None
    }
  }

  private def confsToMap(confs: String): Map[String,String] = {
    confs
        .trim
      .split(" ")
      .map(keyValue => keyValue.split("="))
      .map(keyValue => (keyValue(0), keyValue(1)))
      .toMap
  }

  private def stringToRetentionPolicy(policyName: String): SchemaRetentionPolicy = {
    import za.co.absa.abris.avro.schemas.policy.SchemaRetentionPolicies._
    policyName match {
      case "RETAIN_ORIGINAL_SCHEMA"      => RETAIN_ORIGINAL_SCHEMA
      case "RETAIN_SELECTED_COLUMN_ONLY" => RETAIN_SELECTED_COLUMN_ONLY
      case _ => throw new IllegalArgumentException(s"Invalid schema retention policy name: '$policyName'.")
    }
  }
}
