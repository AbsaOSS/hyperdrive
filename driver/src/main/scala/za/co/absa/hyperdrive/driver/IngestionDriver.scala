/*
 *  Copyright 2019 ABSA Group Limited
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package za.co.absa.hyperdrive.driver

import org.apache.commons.configuration2.Configuration
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.SparkSession
import za.co.absa.hyperdrive.decoder.StreamDecoder
import za.co.absa.hyperdrive.decoder.factories.StreamDecoderAbstractFactory
import za.co.absa.hyperdrive.driver.drivers.CommandLineIngestionDriver.logger
import za.co.absa.hyperdrive.manager.offset.OffsetManager
import za.co.absa.hyperdrive.manager.offset.factories.OffsetManagerAbstractFactory
import za.co.absa.hyperdrive.reader.StreamReader
import za.co.absa.hyperdrive.reader.factories.StreamReaderAbstractFactory
import za.co.absa.hyperdrive.shared.configurations.ConfigurationsKeys.IngestorKeys
import za.co.absa.hyperdrive.transformer.StreamTransformer
import za.co.absa.hyperdrive.transformer.factories.StreamTransformerAbstractFactory
import za.co.absa.hyperdrive.writer.StreamWriter
import za.co.absa.hyperdrive.writer.factories.StreamWriterAbstractFactory

private[driver] class IngestionDriver {

  private val logger = LogManager.getLogger

  def ingest(configuration: Configuration): Unit = {
    logger.info("Ingestion invoked using the configuration below. Going to instantiate components.")
    printConfiguration(configuration)

    val spark = getSparkSession(configuration)
    val streamReader = getStreamReader(configuration)
    val offsetManager = getOffsetManager(configuration)
    val streamDecoder = getStreamDecoder(configuration)
    val streamTransformer = getStreamTransformer(configuration)
    val streamWriter = getStreamWriter(configuration)

    logger.info("Ingestion components instantiated. Going to invoke SparkIngestor.")
    SparkIngestor.ingest(spark, streamReader, offsetManager, streamDecoder, streamTransformer, streamWriter)
  }

  private def getSparkSession(conf: Configuration): SparkSession = SparkSession.builder().appName(conf.getString(IngestorKeys.KEY_APP_NAME)).getOrCreate()

  private def getStreamReader(conf: Configuration): StreamReader = StreamReaderAbstractFactory.getFactory(conf).build(conf)

  private def getOffsetManager(conf: Configuration): OffsetManager = OffsetManagerAbstractFactory.getFactory(conf).build(conf)

  private def getStreamDecoder(conf: Configuration): StreamDecoder = StreamDecoderAbstractFactory.getFactory(conf).build(conf)

  private def getStreamTransformer(conf: Configuration): StreamTransformer = StreamTransformerAbstractFactory.getFactory(conf).build(conf)

  private def getStreamWriter(conf: Configuration): StreamWriter = StreamWriterAbstractFactory.getFactory(conf).build(conf)

  private def printConfiguration(configuration: Configuration): Unit = {
    import scala.collection.JavaConverters._
    configuration
      .getKeys
      .asScala
      .foreach(key => logger.info(s"\t$key = ${configuration.getProperty(key)}"))
  }
}
