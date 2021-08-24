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

package za.co.absa.hyperdrive.ingestor.implementation.writer.kafka

import org.apache.commons.configuration2.Configuration
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import za.co.absa.hyperdrive.ingestor.api.utils.ConfigUtils._
import za.co.absa.hyperdrive.ingestor.api.utils.{ComponentFactoryUtil, StreamWriterUtil}
import za.co.absa.hyperdrive.ingestor.api.writer.{StreamWriter, StreamWriterFactory, StreamWriterProperties}

private[writer] class KafkaStreamWriter(topic: String,
                                        brokers: String,
                                        checkpointLocation: String,
                                        trigger: Trigger,
                                        extraOptions: Map[String, String]) extends StreamWriter {

  def write(dataFrame: DataFrame): StreamingQuery = {
    dataFrame
      .writeStream
      .option(StreamWriterProperties.CheckpointLocation, checkpointLocation)
      .trigger(trigger)
      .option("topic", topic)
      .option("kafka.bootstrap.servers", brokers)
      .options(extraOptions)
      .format("kafka")
      .start()
  }
}

object KafkaStreamWriter extends StreamWriterFactory with KafkaStreamWriterAttributes {

  private val logger = LogManager.getLogger

  override def apply(configuration: Configuration): StreamWriter = {
    logger.info(s"Building ${KafkaStreamWriter.getClass.getCanonicalName}")

    ComponentFactoryUtil.validateConfiguration(configuration, getProperties)
    val topic = configuration.getString(KEY_TOPIC)
    val brokers = configuration.getString(KEY_BROKERS)
    val trigger = StreamWriterUtil.getTrigger(configuration)
    val checkpointLocation = StreamWriterUtil.getCheckpointLocation(configuration)
    val extraOptions = getPropertySubset(configuration, optionalConfKey)

    logger.info(s"Creating writer: topic = '$topic', brokers = '$brokers', trigger = '$trigger'")

    new KafkaStreamWriter(topic, brokers, checkpointLocation, trigger, extraOptions)
  }
}
