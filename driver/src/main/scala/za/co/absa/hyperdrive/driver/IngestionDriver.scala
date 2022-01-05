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

import org.apache.commons.configuration2.Configuration
import org.slf4j.LoggerFactory
import za.co.absa.hyperdrive.ingestor.api.reader.StreamReader
import za.co.absa.hyperdrive.ingestor.api.transformer.StreamTransformer
import za.co.absa.hyperdrive.ingestor.api.writer.StreamWriter
import za.co.absa.hyperdrive.ingestor.implementation.reader.factories.StreamReaderAbstractFactory
import za.co.absa.hyperdrive.ingestor.implementation.transformer.factories.StreamTransformerAbstractFactory
import za.co.absa.hyperdrive.ingestor.implementation.writer.factories.StreamWriterAbstractFactory

private[driver] class IngestionDriver {
  private val logger = LoggerFactory.getLogger(this.getClass)
  val ListDelimiter = ','

  def ingest(configuration: Configuration): Unit = {
    logger.info("Ingestion invoked using the configuration below. Going to instantiate components.")
    printConfiguration(configuration)

    val sparkIngestor = SparkIngestor(configuration)
    val streamReader = getStreamReader(configuration)
    val streamTransformers = getStreamTransformers(configuration)
    val streamWriter = getStreamWriter(configuration)

    logger.info("Ingestion components instantiated. Going to invoke SparkIngestor.")
    sparkIngestor.ingest(streamReader, streamTransformers, streamWriter)
  }

  private def getStreamReader(conf: Configuration): StreamReader = StreamReaderAbstractFactory.build(conf)

  private def getStreamTransformers(conf: Configuration): Seq[StreamTransformer] = StreamTransformerAbstractFactory.build(conf)

  private def getStreamWriter(conf: Configuration): StreamWriter = StreamWriterAbstractFactory.build(conf)

  private def printConfiguration(configuration: Configuration): Unit = {
    import scala.collection.JavaConverters._
    configuration
      .getKeys
      .asScala
      .foreach(key => logger.info(s"\t$key = ${configuration.getProperty(key)}"))
  }
}
