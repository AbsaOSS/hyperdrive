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

import java.net.URI
import org.apache.commons.configuration2.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.DataStreamReader
import za.co.absa.hyperdrive.ingestor.api.reader.{StreamReader, StreamReaderFactory}
import za.co.absa.hyperdrive.ingestor.api.utils.{ConfigUtils, StreamWriterUtil}
import za.co.absa.hyperdrive.ingestor.api.utils.ConfigUtils.{filterKeysContaining, getOrThrow, getSeqOrThrow}
import za.co.absa.hyperdrive.shared.utils.FileUtils

private[reader] object KafkaStreamReaderProps {
  val STREAM_FORMAT_KAFKA_NAME = "kafka"
  val BROKERS_SETTING_KEY = "bootstrap.servers"
  val SPARK_BROKERS_SETTING_KEY = "kafka.bootstrap.servers"
  val TOPIC_SUBSCRIPTION_KEY = "subscribe"
  val WORD_STARTING_OFFSETS = "startingOffsets"
  val STARTING_OFFSETS_EARLIEST = "earliest"
}

/**
 * Creates a stream reader from Kafka.
 *
 * @param topic      String containing the topic
 * @param brokers    String containing the brokers
 * @param extraConfs Extra configurations, e.g. SSL params.
 */
private[reader] class KafkaStreamReader(
  val topic: String, val brokers: String, val checkpointLocation: String, val extraConfs: Map[String, String]) extends StreamReader {
  import KafkaStreamReaderProps._

  private val logger = LoggerFactory.getLogger(this.getClass)()

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
  override def read(spark: SparkSession): DataFrame = {
    implicit val fs: FileSystem = FileSystem.get(new URI(checkpointLocation), spark.sparkContext.hadoopConfiguration)

    if (spark.sparkContext.isStopped) {
      throw new IllegalStateException("SparkSession is stopped.")
    }

    logger.info(s"Will read from topic $topic")

    val streamReader = spark
      .readStream
      .format(STREAM_FORMAT_KAFKA_NAME)
      .option(TOPIC_SUBSCRIPTION_KEY, topic)
      .option(SPARK_BROKERS_SETTING_KEY, brokers)

    val streamReaderWithStartingOffsets = configureStartingOffsets(streamReader)
    streamReaderWithStartingOffsets
      .options(extraConfs)
      .load()
  }

  private def configureStartingOffsets(streamReader: DataStreamReader)(implicit fileSystem: FileSystem): DataStreamReader = {
    val startingOffsets = getStartingOffsets(checkpointLocation)

    startingOffsets match {
      case Some(startOffset) =>
        logger.info(s"Setting starting offsets for $startOffset.")
        streamReader.option(WORD_STARTING_OFFSETS, startOffset)
      case _ =>
        logger.info(s"No offsets to set for.")
        streamReader
    }
  }

  private def getStartingOffsets(checkpointLocation: String)(implicit fileSystem: FileSystem): Option[String] = {
    if (FileUtils.exists(checkpointLocation) && !FileUtils.isEmpty(checkpointLocation)) {
      Option.empty
    }
    else {
      Option(STARTING_OFFSETS_EARLIEST)
    }
  }

}

object KafkaStreamReader extends StreamReaderFactory with KafkaStreamReaderAttributes {
  private val logger = LoggerFactory.getLogger(this.getClass)

  override def apply(conf: Configuration): StreamReader = {
    val topic = getTopic(conf)
    val brokers = getBrokers(conf)
    val extraOptions = getExtraOptions(conf)
    val checkpointLocation = StreamWriterUtil.getCheckpointLocation(conf)

    logger.info(s"Going to create KafkaStreamReader with: topic='$topic', brokers='$brokers', extraOptions=${filterKeysContaining(extraOptions, exclusionToken = "password")}")

    new KafkaStreamReader(topic, brokers, checkpointLocation, extraOptions)
  }

  private def getTopic(configuration: Configuration): String = {
    getOrThrow(KEY_TOPIC, configuration, errorMessage = s"Topic not found. Is $KEY_TOPIC defined?")
  }

  private def getBrokers(configuration: Configuration): String = {
    val brokers = getSeqOrThrow(KEY_BROKERS, configuration, errorMessage = s"Brokers not found. Is $KEY_BROKERS defined?")
    brokers.mkString(",")
  }

  private def getExtraOptions(configuration: Configuration): Map[String, String] = ConfigUtils.getPropertySubset(configuration, getExtraConfigurationPrefix.get)
}
