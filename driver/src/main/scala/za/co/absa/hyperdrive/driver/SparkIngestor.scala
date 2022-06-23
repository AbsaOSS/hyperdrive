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
import org.slf4j.LoggerFactory
import org.apache.spark.sql.SparkSession
import za.co.absa.hyperdrive.driver.TerminationMethodEnum.{AwaitTermination, ProcessAllAvailable, TerminationMethod}
import za.co.absa.hyperdrive.ingestor.api.reader.StreamReader
import za.co.absa.hyperdrive.ingestor.api.transformer.StreamTransformer
import za.co.absa.hyperdrive.ingestor.api.utils.{ComponentFactoryUtil, ConfigUtils}
import za.co.absa.hyperdrive.ingestor.api.writer.StreamWriter
import za.co.absa.hyperdrive.shared.exceptions.{IngestionException, IngestionStartException}

import scala.util.control.NonFatal
import scala.util.{Failure, Success}

/**
 * This object is responsible for running the ingestion job by using the components it
 * receives upon invocation.
 */
class SparkIngestor(val spark: SparkSession,
                    val terminationMethod: TerminationMethod,
                    val awaitTerminationTimeout: Option[Long],
                    val conf: Configuration) {

  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * This method performs the ingestion according to the components it receives.
   *
   * THIS METHOD IS BLOCKING, which is achieved by invoking "awaitTermination" on the streaming query, thus, if you
   * do not want a blocking behaviour, make sure you invoke it from inside a separate thread (or similar approach).
   *
   * @param streamReader       [[StreamReader]] implementation responsible for connecting to the source stream.
   * @param streamTransformers List of [[StreamTransformer]] implementation responsible for performing any transformations on the stream data (e.g. conformance)
   * @param streamWriter       [[StreamWriter]] implementation responsible for defining how and where the stream will be sent.
   */
  @throws(classOf[IllegalArgumentException])
  @throws(classOf[IngestionStartException])
  @throws(classOf[IngestionException])
  def ingest(streamReader: StreamReader,
             streamTransformers: Seq[StreamTransformer],
             streamWriter: StreamWriter): Unit = {

    val ingestionId = generateIngestionId

    logger.info(s"STARTING ingestion (id = $ingestionId)")

    val ingestionQuery = try {
      val inputDataframe = streamReader.read(spark)
      val transformedDataFrame = streamTransformers.foldLeft(inputDataframe)((dataFrame, streamTransformer) => streamTransformer.transform(dataFrame))
      streamWriter.write(transformedDataFrame)
    } catch {
      case NonFatal(e) =>
        throw new IngestionStartException(s"NOT STARTED ingestion $ingestionId. This exception was thrown during the starting of the ingestion job. Check the logs for details.", e)
    }

    try {
      terminationMethod match {
        case ProcessAllAvailable =>
          ingestionQuery.processAllAvailable()
          ingestionQuery.stop()
        case AwaitTermination =>
          awaitTerminationTimeout match {
            case Some(timeout) =>
              ingestionQuery.awaitTermination(timeout)
              ingestionQuery.stop()
            case None =>
              ingestionQuery.awaitTermination()
          }
      }
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

  private val logger = LoggerFactory.getLogger(this.getClass)

  def apply(conf: Configuration): SparkIngestor = {
    ComponentFactoryUtil.validateConfiguration(conf, getProperties)
    val spark = SparkSession.builder().getOrCreate()
    val terminationMethod = getTerminationMethod(conf)
    val awaitTerminationTimeout = getAwaitTerminationTimeoutMs(conf)

    logger.info(s"Creating ingestor: termination method = '$terminationMethod', " +
      s"await termination timeout = '$awaitTerminationTimeout'")
    new SparkIngestor(spark, terminationMethod, awaitTerminationTimeout, conf)
  }

  private def getTerminationMethod(conf: Configuration): TerminationMethod = {
    ConfigUtils.getOrNone(KEY_TERMINATION_METHOD, conf) match {
      case Some(name) => parseTerminationMethod(name)
      case None => AwaitTermination
    }
  }

  private def parseTerminationMethod(name: String) = {
     TerminationMethodEnum.of(name) match {
      case Failure(exception) => throw new IllegalArgumentException(s"Invalid value for $KEY_TERMINATION_METHOD", exception)
      case Success(value) => value
    }
  }

  private def getAwaitTerminationTimeoutMs(conf: Configuration): Option[Long] = {
    ConfigUtils.getOrNone(KEY_AWAIT_TERMINATION_TIMEOUT, conf)
      .flatMap(value =>
        try {
          Some(value.toLong)
        } catch {
          case e: NumberFormatException => throw new IllegalArgumentException(s"Invalid value for " +
            s"$KEY_AWAIT_TERMINATION_TIMEOUT. Value: $value", e)
        }
      )
  }
}
