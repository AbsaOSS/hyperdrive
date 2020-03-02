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

package ${package}.manager.mycomponent

import org.apache.commons.configuration2.Configuration
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter}
import za.co.absa.hyperdrive.ingestor.api.manager.{StreamManager, StreamManagerFactory, StreamManagerFactoryProvider}
import za.co.absa.hyperdrive.ingestor.api.{HasComponentAttributes, PropertyMetadata}

/**
 * This is a stub for a custom implementation of a StreamManager
 */

private[manager] class MyStreamManagerImpl extends StreamManager {

  def configure(streamReader: DataStreamReader, configuration: org.apache.hadoop.conf.Configuration): DataStreamReader = ???

  def configure(streamWriter: DataStreamWriter[Row], configuration: org.apache.hadoop.conf.Configuration): DataStreamWriter[Row] = ???
}

object MyStreamManagerImpl extends StreamManagerFactory with MyStreamManagerImplAttributes {
  private val logger = LogManager.getLogger

  override def apply(conf: Configuration): StreamManager = {
    logger.info("Building MyStreamManagerImpl")
    new MyStreamManagerImpl()
  }
}

trait MyStreamManagerImplAttributes extends HasComponentAttributes {

  override def getName: String = "My Stream Manager"

  override def getDescription: String = "This component is a stub"

  override def getProperties: Map[String, PropertyMetadata] = Map()

  override def getExtraConfigurationPrefix: Option[String] = None

}

class MyStreamManagerImplLoader extends StreamManagerFactoryProvider {
  override def getComponentFactory: StreamManagerFactory = MyStreamManagerImpl
}
