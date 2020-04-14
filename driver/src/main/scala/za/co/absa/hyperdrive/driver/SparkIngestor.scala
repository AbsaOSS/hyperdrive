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

package za.co.absa.hyperdrive.driver

import java.util.UUID

import org.apache.commons.configuration2.Configuration
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.SparkSession
import za.co.absa.hyperdrive.ingestor.api.decoder.StreamDecoder
import za.co.absa.hyperdrive.ingestor.api.manager.StreamManager
import za.co.absa.hyperdrive.ingestor.api.reader.StreamReader
import za.co.absa.hyperdrive.ingestor.api.transformer.StreamTransformer
import za.co.absa.hyperdrive.ingestor.api.utils.ComponentFactoryUtil
import za.co.absa.hyperdrive.ingestor.api.writer.StreamWriter
import za.co.absa.hyperdrive.shared.exceptions.{IngestionException, IngestionStartException}
import za.co.absa.hyperdrive.shared.utils.ConfigUtils

import scala.util.control.NonFatal

/**
  * This object is responsible for running the ingestion job by using the components it
  * receives upon invocation.
  */
class SparkIngestor(val spark: SparkSession,
                    val conf: Configuration) {

  private val logger = LogManager.getLogger

  /**
    * This method performs the ingestion according to the components it receives.
    *
    * THIS METHOD IS BLOCKING, which is achieved by invoking "processAllAvailable" on the streaming query, thus, if you
    * do not want a blocking behaviour, make sure you invoke it from inside a separate thread (or similar approach).
    *
    * IF this method is invoked to ingest from a continuous source (e.g. a topic that is receiving data no-stop), it WILL
    * BLOCK UNTIL THERE IS NO MORE DATA because of how "processAllAvailable" works.
    *
    * @param streamReader      [[StreamReader]] implementation responsible for connecting to the source stream.
    * @param streamManager     [[StreamManager]] implementation responsible for cross-cutting concerns, e.g. defining offsets on the source stream and checkpoints on the destination stream.
    * @param decoder           [[StreamDecoder]] implementation responsible for handling differently encoded payloads.
    * @param streamTransformer [[za.co.absa.hyperdrive.ingestor.api.transformer.StreamTransformer]] implementation responsible for performing any transformations on the stream data (e.g. conformance)
    * @param streamWriter      [[za.co.absa.hyperdrive.ingestor.api.writer.StreamWriter]] implementation responsible for defining how and where the stream will be sent.
    */
  @throws(classOf[IllegalArgumentException])
  @throws(classOf[IngestionStartException])
  @throws(classOf[IngestionException])
  def ingest(streamReader: StreamReader,
             streamManager: StreamManager,
             decoder: StreamDecoder,
             streamTransformer: StreamTransformer,
             streamWriter: StreamWriter): Unit= {

    val ingestionId = generateIngestionId

    logger.info(s"STARTING ingestion (id = $ingestionId)")

    val ingestionQuery = try {
      val inputStreamReader = streamReader.read(spark) // gets the source stream
      val configuredStreamReader = streamManager.configure(inputStreamReader, spark.sparkContext.hadoopConfiguration) // configures DataStreamReader and DataStreamWriter
      val decodedDataFrame = decoder.decode(configuredStreamReader) // decodes the payload from whatever encoding it has
      val transformedDataFrame = streamTransformer.transform(decodedDataFrame) // applies any transformations to the data
      streamWriter.write(transformedDataFrame, streamManager) // sends the stream to the destination
    } catch {
      case NonFatal(e) =>
        throw new IngestionStartException(s"NOT STARTED ingestion $ingestionId. This exception was thrown during the starting of the ingestion job. Check the logs for details.", e)
    }

    try {
      ingestionQuery.processAllAvailable() // processes everything available at the source and stops after that
      ingestionQuery.stop()
    } catch {
      case NonFatal(e) =>
        throw new IngestionException(message = s"PROBABLY FAILED INGESTION $ingestionId. There was no error in the query plan, but something when wrong. " +
          s"Pay attention to this exception since the query has been started, which might lead to duplicate data or similar issues. " +
          s"The logs should have enough detail, but a possible course of action is to replay this ingestion and overwrite the destination.", e)
    }

    logger.info(s"FINISHED ingestion (id = $ingestionId)")
  }

  private def generateIngestionId: String = UUID.randomUUID().toString
}

object SparkIngestor extends SparkIngestorAttributes {
  def apply(conf: Configuration): SparkIngestor = {
    ComponentFactoryUtil.validateConfiguration(conf, getProperties)
    val spark = getSparkSession(conf)
    new SparkIngestor(spark, conf)
  }

  private def getSparkSession(conf: Configuration): SparkSession = {
    val name = ConfigUtils.getOrThrow(KEY_APP_NAME, conf)
    val sparkBuilder = SparkSession.builder().appName(name)
    val extraConf = ConfigUtils.getPropertySubset(conf, optionalConfKey)
    val sparkBuilderWithExtraConf = extraConf.foldLeft(sparkBuilder)((sparkBuilder, keyValue) =>
      sparkBuilder.config(keyValue._1, keyValue._2))
    sparkBuilderWithExtraConf.getOrCreate()
  }

}
