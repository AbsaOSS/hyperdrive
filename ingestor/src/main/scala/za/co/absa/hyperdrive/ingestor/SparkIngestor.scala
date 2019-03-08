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
import za.co.absa.hyperdrive.transformer.encoding.AvroDecoder
import za.co.absa.hyperdrive.writer.StreamWriter

object SparkIngestor {

  private val logger = LogManager.getLogger

  def ingest(topic: String)
            (streamReader: StreamReader)
            (offsetManager: OffsetManager)
            (avroDecoder: AvroDecoder)
            (streamTransformer: StreamTransformer)
            (streamWriter: StreamWriter): Unit= {

    logger.info(s"STARTING ingestion from topic '$topic' into destination '${streamWriter.getDestination}'")

    val spark = SparkSession.builder().appName(s"SparkIngestor-$topic").master("local[*]").getOrCreate()

    val inputStream = streamReader.read(spark)
    val configuredStreamReader = offsetManager.configureOffsets(inputStream)
    val decodedAvroDataFrame = avroDecoder.decode(configuredStreamReader)
    val transformedAvroDataFrame = streamTransformer.transform(decodedAvroDataFrame)

    val ingestionQuery = streamWriter.write(transformedAvroDataFrame, offsetManager)

    ingestionQuery.processAllAvailable()
    ingestionQuery.stop()

    logger.info("Going to sleep")
    logger.info(s"FINISHED ingestion from topic '$topic' into '${streamWriter.getDestination}'")


  }
}
