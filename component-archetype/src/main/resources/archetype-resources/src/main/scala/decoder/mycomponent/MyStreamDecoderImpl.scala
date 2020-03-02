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

package ${package}.decoder.mycomponent

import org.apache.commons.configuration2.Configuration
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.DataStreamReader
import za.co.absa.hyperdrive.ingestor.api.decoder.{StreamDecoder, StreamDecoderFactory, StreamDecoderFactoryProvider}
import za.co.absa.hyperdrive.ingestor.api.{HasComponentAttributes, PropertyMetadata}

/**
 * This is a stub for a custom implementation of a StreamDecoder
 */

private[decoder] class MyStreamDecoderImpl extends StreamDecoder {
  def decode(streamReader: DataStreamReader): DataFrame = ???
}

object MyStreamDecoderImpl extends StreamDecoderFactory with MyStreamDecoderImplAttributes {
  private val logger = LogManager.getLogger

  override def apply(conf: Configuration): StreamDecoder = {
    logger.info("Building MyStreamDecoderImpl")
    new MyStreamDecoderImpl()
  }
}

trait MyStreamDecoderImplAttributes extends HasComponentAttributes {

  override def getName: String = "My Stream Decoder"

  override def getDescription: String = "This component is a stub"

  override def getProperties: Map[String, PropertyMetadata] = Map()

  override def getExtraConfigurationPrefix: Option[String] = None
}

class MyStreamDecoderImplLoader extends StreamDecoderFactoryProvider {
  override def getComponentFactory: StreamDecoderFactory = MyStreamDecoderImpl
}
