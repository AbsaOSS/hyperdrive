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

package za.co.absa.hyperdrive.ingestor

import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.codehaus.plexus.logging.LoggerManager
import za.co.absa.hyperdrive.ingestor.configuration.CompositeIngestionConfigurations
import za.co.absa.hyperdrive.ingestor.configuration.components._
import za.co.absa.hyperdrive.manager.offset.OffsetManager
import za.co.absa.hyperdrive.manager.offset.impl.CheckpointingOffsetManager
import za.co.absa.hyperdrive.reader.StreamReader
import za.co.absa.hyperdrive.reader.impl.KafkaStreamReader
import za.co.absa.hyperdrive.transformer.data.StreamTransformer
import za.co.absa.hyperdrive.transformer.data.impl.SelectAllStreamTransformer
import za.co.absa.hyperdrive.transformer.encoding.StreamDecoder
import za.co.absa.hyperdrive.transformer.encoding.impl.AvroStreamDecoder
import za.co.absa.hyperdrive.writer.StreamWriter
import za.co.absa.hyperdrive.writer.impl.ParquetStreamWriter

class IngestionDriver {

  private val logger = LogManager.getLogger

  def ingest(configurations: CompositeIngestionConfigurations): Unit = {
    logger.info("Ingestion invoked. Going to instantiate components.")
    val spark = getSparkSession(configurations.sparkConf)
    val streamReader = getStreamReader(configurations.streamReaderConf)
    val offsetManager = getOffsetManager(configurations.offsetManagerConf)
    val streamDecoder = getStreamDecoder(configurations.streamDecoderConf)
    val streamTransformer = getStreamTransformer(configurations.streamTransformerConf)
    val streamWriter = getStreamWriter(configurations.streamWriterConf)

    logger.info("Ingestion components instantiated. Going to invoke SparkIngestor.")
    SparkIngestor.ingest(spark, streamReader, offsetManager, streamDecoder, streamTransformer, streamWriter)
  }

  private def getSparkSession(conf: SparkConf): SparkSession = SparkSession.builder().appName(conf.appName).getOrCreate()

  private def getStreamReader(conf: KafkaStreamReaderConf): StreamReader = new KafkaStreamReader(conf.topic, conf.brokers, conf.extraConfs)

  private def getOffsetManager(conf: CheckpointingOffsetManagerConf): OffsetManager = new CheckpointingOffsetManager(conf.topic, conf.checkpointBaseLocation, conf.configuration)

  private def getStreamDecoder(conf: AvroStreamDecoderConf): StreamDecoder = new AvroStreamDecoder(conf.schemaRegistrySettings, conf.retentionPolicy)

  private def getStreamTransformer(conf: SelectAllStreamTransformerConf): StreamTransformer = new SelectAllStreamTransformer

  private def getStreamWriter(conf: ParquetStreamWriterConf): StreamWriter = new ParquetStreamWriter(conf.destination, conf.extraConfOptions)
}
