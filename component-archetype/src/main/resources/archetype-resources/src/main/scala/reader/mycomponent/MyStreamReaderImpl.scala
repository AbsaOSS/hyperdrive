/*
 *  Copyright 2018 ABSA Group Limited
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

package ${package}.reader.mycomponent

import org.apache.commons.configuration2.Configuration
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.DataStreamReader
import za.co.absa.hyperdrive.ingestor.api.reader.{StreamReader, StreamReaderFactory, StreamReaderFactoryProvider}
import za.co.absa.hyperdrive.ingestor.api.PropertyMetadata


/**
 * This is a stub for a custom implementation of a StreamReader
 */

private[reader] class MyStreamReaderImpl() extends StreamReader {
  override def read(spark: SparkSession): DataStreamReader = ???
}

object MyStreamReaderImpl extends StreamReaderFactory {
  private val logger = LogManager.getLogger

  override def apply(conf: Configuration): StreamReader = {
    logger.info("Building MyStreamReaderImpl")
    new MyStreamReaderImpl()
  }

  override def getName: String = "My Stream Reader"

  override def getDescription: String = "This component is a stub"

  override def getProperties: Map[String, PropertyMetadata] = Map()

  override def getExtraConfigurationPrefix: Option[String] = None
}

class MyStreamReaderImplLoader extends StreamReaderFactoryProvider {
  override def getComponentFactory: StreamReaderFactory = MyStreamReaderImpl
}
