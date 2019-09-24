/*
 * Copyright 2018-2019 ABSA Group Limited
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

package za.co.absa.hyperdrive.ingestor.implementation.reader.kafka

import org.apache.commons.configuration2.Configuration
import org.apache.commons.lang3.StringUtils
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.DataStreamReader
import za.co.absa.hyperdrive.ingestor.api.reader.{StreamReader, StreamReaderFactory}
import za.co.absa.hyperdrive.shared.configurations.ConfigurationsKeys.KafkaStreamReaderKeys.{KEY_BROKERS, KEY_TOPIC, rootComponentConfKey, rootFactoryOptionalConfKey}
import za.co.absa.hyperdrive.shared.utils.ConfigUtils.{getOrNone, getOrThrow, getSeqOrThrow}

private[reader] object KafkaStreamReaderProps {
  val STREAM_FORMAT_KAFKA_NAME  = "kafka"
  val BROKERS_SETTING_KEY       = "bootstrap.servers"
  val SPARK_BROKERS_SETTING_KEY = "kafka.bootstrap.servers"
  val TOPIC_SUBSCRIPTION_KEY    = "subscribe"
}

/**
  * Creates a stream reader from Kafka.
  *
  * @param topic String containing the topic
  * @param brokers String containing the brokers
  * @param extraConfs Extra configurations, e.g. SSL params.
  */
private[reader] class KafkaStreamReader(val topic: String, val brokers: String, val extraConfs: Map[String,String]) extends StreamReader {

  private val logger = LogManager.getLogger()

  if (StringUtils.isBlank(topic)) {
    throw new IllegalArgumentException(s"Invalid topic: '$topic'")
  }

  if (StringUtils.isBlank(brokers)) {
    throw new IllegalArgumentException(s"Invalid brokers: '$brokers'")
  }

  if (extraConfs == null) {
    throw new IllegalArgumentException("Null extra configurations.")
  }

  /**
    * Creates a [[DataStreamReader]] instance from a SparkSession
    *
    * IMPORTANT: this method does not check against malformed data (e.g. invalid broker protocols or certificate locations),
    * thus, if not properly configured, the issue will ONLY BE FOUND AT RUNTIME.
    */
  override def read(spark: SparkSession): DataStreamReader = {

    import KafkaStreamReaderProps._

    if (spark == null) {
      throw new IllegalArgumentException("Null SparkSession instance.")
    }

    if (spark.sparkContext.isStopped) {
      throw new IllegalStateException("SparkSession is stopped.")
    }

    val streamReader = spark
      .readStream
      .format(STREAM_FORMAT_KAFKA_NAME)
      .option(TOPIC_SUBSCRIPTION_KEY, topic)
      .option(SPARK_BROKERS_SETTING_KEY, brokers)

    extraConfs.foldLeft(streamReader) {
      case (previousStreamReader, (newConfKey, newConfValue)) => previousStreamReader.option(newConfKey, newConfValue)
    }
  }

  override def getSourceName: String = s"Kafka topic: $topic"
}

object KafkaStreamReader extends StreamReaderFactory {
  private val logger = LogManager.getLogger

  override def apply(conf: Configuration): StreamReader = {
    val topic = getTopic(conf)
    val brokers = getBrokers(conf)
    val extraOptions = getExtraOptions(conf)

    logger.info(s"Going to create KafkaStreamReader with: topic='$topic', brokers='$brokers', extraOptions=${filterKeysContaining(extraOptions, exclusionToken = "password")}")

    new KafkaStreamReader(topic, brokers, extraOptions)
  }

  private def getTopic(configuration: Configuration): String = {
    getOrThrow(KEY_TOPIC, configuration, errorMessage = s"Topic not found. Is $KEY_TOPIC defined?")
  }

  private def getBrokers(configuration: Configuration): String = {
    val brokers = getSeqOrThrow(KEY_BROKERS, configuration, errorMessage = s"Brokers not found. Is $KEY_BROKERS defined?")
    brokers.mkString(",")
  }

  private def getExtraOptions(configuration: Configuration): Map[String,String] = {
    val optionalKeys = getKeysFromPrefix(configuration, rootFactoryOptionalConfKey)

    val extraConfs = optionalKeys.foldLeft(Map[String,String]()) {
      case (map,securityKey) =>
        getOrNone(securityKey, configuration) match {
          case Some(value) => map + (tweakOptionKeyName(securityKey) -> value)
          case None => map
        }
    }

    if (extraConfs.isEmpty || extraConfs.size == optionalKeys.size) {
      extraConfs
    }
    else {
      logger.warn(s"Assuming no security settings, since some appear to be missing: {${findMissingKeys(optionalKeys, extraConfs)}}")
      Map[String,String]()
    }
  }

  private def getKeysFromPrefix(configuration: Configuration, prefix: String): Seq[String] = {
    val optionalKeys = configuration.getKeys(prefix)

    if (optionalKeys != null) {
      import scala.collection.JavaConverters._
      optionalKeys.asScala.toSeq
    } else {
      Seq[String]()
    }
  }

  private def findMissingKeys(keys: Seq[String], map: Map[String,String]): Seq[String] = keys.filterNot(map.contains(_))

  private def tweakKeyName(key: String): String = {
    key.replace(s"$rootComponentConfKey.", "") // remove the component root configuration key
  }

  private def tweakOptionKeyName(key: String): String = {
    key.replace(s"$rootFactoryOptionalConfKey.", "") // remove the component root.option configuration key
  }

  private def filterKeysContaining(map: Map[String,String], exclusionToken: String): Map[String,String] = map.filterKeys(!_.contains(exclusionToken))
}
