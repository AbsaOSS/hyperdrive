/*
 *
 * Copyright 2019 ABSA Group Limited
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package za.co.absa.hyperdrive.ingestor

import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.SparkSession
import za.co.absa.hyperdrive.manager.offset.OffsetManager
import za.co.absa.hyperdrive.reader.StreamReader
import za.co.absa.hyperdrive.transformer.data.StreamTransformer
import za.co.absa.hyperdrive.transformer.encoding.StreamDecoder
import za.co.absa.hyperdrive.writer.StreamWriter

/**
  * This object is responsible for running the ingestion job by using the components it
  * receives upon invocation.
  */
object SparkIngestor {

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
    * @param spark [[SparkSession]] instance.
    * @param streamReader [[StreamReader]] implementation responsible for connecting to the source stream.
    * @param offsetManager [[OffsetManager]] implementation responsible for defining offsets on the source stream and checkpoints on the destination stream.
    * @param decoder [[StreamDecoder]] implementation responsible for handling differently encoded payloads.
    * @param streamTransformer [[StreamTransformer]] implementation responsible for performing any transformations on the stream data (e.g. conformance)
    * @param streamWriter [[StreamWriter]] implementation responsible for defining how and where the stream will be sent.
    */
  def ingest(spark: SparkSession)
            (streamReader: StreamReader)
            (offsetManager: OffsetManager)
            (decoder: StreamDecoder)
            (streamTransformer: StreamTransformer)
            (streamWriter: StreamWriter): Unit= {

    if (spark == null) {
      throw new IllegalArgumentException("Received NULL SparkSession instance.")
    }

    if (streamReader == null) {
      throw new IllegalArgumentException("Received NULL StreamReader instance.")
    }

    if (offsetManager == null) {
      throw new IllegalArgumentException("Received NULL OffsetManager instance.")
    }

    if (decoder == null) {
      throw new IllegalArgumentException("Received NULL AvroDecoder instance.")
    }

    if (streamTransformer == null) {
      throw new IllegalArgumentException("Received NULL StreamTransformer instance.")
    }

    if (streamWriter == null) {
      throw new IllegalArgumentException("Received NULL StreamWriter instance.")
    }

    logger.info(s"STARTING ingestion from '${streamReader.getSourceName}' into '${streamWriter.getDestination}'")

    val inputStream = streamReader.read(spark) // gets the source stream
    val configuredStreamReader = offsetManager.configureOffsets(inputStream) // does offset management if any
    val decodedDataFrame = decoder.decode(configuredStreamReader) // decodes the payload from whatever encoding it has
    val transformedDataFrame = streamTransformer.transform(decodedDataFrame) // applies any transformations to the data

    val ingestionQuery = streamWriter.write(transformedDataFrame, offsetManager) // sends the stream to the destination

    ingestionQuery.processAllAvailable() // processes everything available at the source and stops after that
    ingestionQuery.stop()

    logger.info(s"FINISHED ingestion from '${streamReader.getSourceName}' into '${streamWriter.getDestination}'")
  }
}
