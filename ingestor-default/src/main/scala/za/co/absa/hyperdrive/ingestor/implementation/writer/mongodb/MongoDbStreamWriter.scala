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

package za.co.absa.hyperdrive.ingestor.implementation.writer.mongodb

import org.apache.commons.configuration2.Configuration
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import za.co.absa.hyperdrive.ingestor.api.utils.{ConfigUtils, StreamWriterUtil}
import za.co.absa.hyperdrive.ingestor.api.writer.{StreamWriter, StreamWriterFactory, StreamWriterProperties}

private[writer] class MongoDbStreamWriter(trigger: Trigger,
                                          checkpointLocation: String,
                                          uri: String,
                                          database: Option[String],
                                          collection: Option[String],
                                          val extraConfOptions: Map[String, String]) extends StreamWriter {
  private val logger = LogManager.getLogger
  if (!uri.toLowerCase.startsWith("mongodb://")) {
    throw new IllegalArgumentException(s"Invalid MongoDB URI: '$uri'. It should start with 'mongodb://'.")
  }

  override def write(dataFrame: DataFrame): StreamingQuery = {
    val options = extraConfOptions ++
      database.map(db => ("spark.mongodb.output.database", db)) ++
      collection.map(c => ("spark.mongodb.output.collection", c))

    logger.info(s"Writing to $uri")
    if (options.nonEmpty) {
      val optionsStr = options.map { case (k, v) => s"$k='$v" }.mkString(", ")
      logger.info(s"Options: $optionsStr")
    }

    /* This is how this should work when MongoDB Spark connector starts supporting streaming write:
    dataFrame.writeStream
      .trigger(trigger)
      .format("mongo")
      .option("spark.mongodb.output.uri", uri)
      .outputMode(OutputMode.Append())
      .options(extraConfOptions)
      .option(StreamWriterProperties.CheckpointLocation, checkpointLocation)
      .start()
     */
    dataFrame.writeStream
      .trigger(trigger)
      .outputMode(OutputMode.Append())
      .option(StreamWriterProperties.CheckpointLocation, checkpointLocation)
      .options(extraConfOptions)
      .foreachBatch((df, batchId) => {
        logger.info(s"Writing batchId: $batchId")
        df.write
          .mode(SaveMode.Append)
          .format("mongo")
          .option("spark.mongodb.output.uri", uri)
          .options(extraConfOptions)
          .save()
      })
      .start()
  }

  def getDestination: String = uri
}

object MongoDbStreamWriter extends StreamWriterFactory with MongoDbStreamWriterAttributes {

  def apply(config: Configuration): StreamWriter = {
    val trigger = StreamWriterUtil.getTrigger(config)
    val checkpointLocation = StreamWriterUtil.getCheckpointLocation(config)

    val uri = getUri(config)
    val database = ConfigUtils.getOrNone(KEY_DATABASE, config)
    val collection = ConfigUtils.getOrNone(KEY_COLLECTION, config)

    val extraOptions = getExtraOptions(config)

    val dbOptions = Seq(
      database.map(db => ("database", db)),
      collection.map(c => ("collection", c))
    ).flatMap {
      case Some((k, v)) => Some(s"$k='$v'")
      case None => None
    }
      .mkString(", ", ", ", "")

    LogManager.getLogger.info(s"Going to create MongoDbStreamWriter instance using: " +
      s"trigger='$trigger', checkpointLocation='$checkpointLocation', url='$uri'$dbOptions, extra options='$extraOptions'")

    new MongoDbStreamWriter(trigger, checkpointLocation, uri, database, collection, extraOptions)
  }

  def getUri(configuration: Configuration): String = ConfigUtils.getOrThrow(KEY_URI, configuration, errorMessage = s"Output MongoDB URI is not specified. Is '$KEY_URI' defined?")

  def getExtraOptions(configuration: Configuration): Map[String, String] = ConfigUtils.getPropertySubset(configuration, KEY_EXTRA_CONFS_ROOT)

  override def getExtraConfigurationPrefix: Option[String] = Some(KEY_EXTRA_CONFS_ROOT)
}

