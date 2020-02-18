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

package za.co.absa.hyperdrive.ingestor.implementation.reader.kafka

import org.apache.commons.configuration2.Configuration
import org.apache.commons.lang3.StringUtils
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.DataStreamReader
import za.co.absa.hyperdrive.ingestor.api.PropertyMetadata
import za.co.absa.hyperdrive.ingestor.api.reader.{StreamReader, StreamReaderFactory, StreamReaderFactoryProvider}
import za.co.absa.hyperdrive.shared.configurations.ConfigurationsKeys.KafkaStreamReaderKeys.{KEY_BROKERS, KEY_TOPIC, rootFactoryOptionalConfKey}
import za.co.absa.hyperdrive.shared.utils.ConfigUtils
import za.co.absa.hyperdrive.shared.utils.ConfigUtils.{getOrThrow, getSeqOrThrow}

private[reader] object KafkaStreamReaderProps {
  val STREAM_FORMAT_KAFKA_NAME = "kafka"
  val BROKERS_SETTING_KEY = "bootstrap.servers"
  val SPARK_BROKERS_SETTING_KEY = "kafka.bootstrap.servers"
  val TOPIC_SUBSCRIPTION_KEY = "subscribe"
}

/**
 * Creates a stream reader from Kafka.
 *
 * @param topic      String containing the topic
 * @param brokers    String containing the brokers
 * @param extraConfs Extra configurations, e.g. SSL params.
 */
private[reader] class KafkaStreamReader(val topic: String, val brokers: String, val extraConfs: Map[String, String]) extends StreamReader {

  private val logger = LogManager.getLogger()

  if (StringUtils.isBlank(topic)) {
    throw new IllegalArgumentException(s"Invalid topic: '$topic'")
  }

  if (StringUtils.isBlank(brokers)) {
    throw new IllegalArgumentException(s"Invalid brokers: '$brokers'")
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

    logger.info(s"Will read from topic $topic")

    val streamReader = spark
      .readStream
      .format(STREAM_FORMAT_KAFKA_NAME)
      .option(TOPIC_SUBSCRIPTION_KEY, topic)
      .option(SPARK_BROKERS_SETTING_KEY, brokers)

    streamReader.options(extraConfs)
  }
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

  override def getName: String = "Kafka Reader"

  override def getDescription: String = "This component ingests data from a kafka topic."

  override def getProperties: Map[String, PropertyMetadata] = Map(
    KEY_TOPIC -> PropertyMetadata("Topic name", Some("Name of the kafka topic"), required = true),
    KEY_BROKERS -> PropertyMetadata("Brokers", Some("Comma-separated list of kafka broker urls"), required = true))

  override def getExtraConfigurationPrefix: Option[String] = Some(rootFactoryOptionalConfKey)

  private def getTopic(configuration: Configuration): String = {
    getOrThrow(KEY_TOPIC, configuration, errorMessage = s"Topic not found. Is $KEY_TOPIC defined?")
  }

  private def getBrokers(configuration: Configuration): String = {
    val brokers = getSeqOrThrow(KEY_BROKERS, configuration, errorMessage = s"Brokers not found. Is $KEY_BROKERS defined?")
    brokers.mkString(",")
  }

  private def getExtraOptions(configuration: Configuration): Map[String, String] = ConfigUtils.getPropertySubset(configuration, rootFactoryOptionalConfKey)

  private def filterKeysContaining(map: Map[String, String], exclusionToken: String) = map.filterKeys(!_.contains(exclusionToken))
}

class KafkaStreamReaderLoader extends StreamReaderFactoryProvider {
  override def getComponentFactory: StreamReaderFactory = KafkaStreamReader
}
