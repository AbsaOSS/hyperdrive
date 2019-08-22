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

package za.co.absa.hyperdrive.ingestor.implementation.transformer.factories

import org.apache.commons.configuration2.Configuration
import org.apache.logging.log4j.LogManager
import za.co.absa.hyperdrive.ingestor.api.transformer.StreamTransformer
import za.co.absa.hyperdrive.ingestor.implementation.transformer.StreamTransformerFactory
import za.co.absa.hyperdrive.ingestor.implementation.transformer.factories.column.selection.ColumnSelectorStreamTransformerFactory

import za.co.absa.hyperdrive.shared.utils.ClassLoaderUtils

/**
  * Abstract factory for stream transformers.
  *
  * After creating a new StreamTransformer implementation, add the corresponding factory to "factoryMap" inside this class.
  */
object StreamTransformerAbstractFactory {

  private val logger = LogManager.getLogger
  val componentConfigKey = "component.transformer"

  private val factoryMap = Map[String,StreamTransformerFactory](
    ColumnSelectorStreamTransformerFactory.name.toLowerCase -> ColumnSelectorStreamTransformerFactory)

  def build(config: Configuration): StreamTransformer = {

    logger.info(s"Going to load factory for configuration '$componentConfigKey'.")

    val factoryName = config.getString(componentConfigKey)
    val factory = ClassLoaderUtils.loadSingletonClassOfType[StreamTransformerFactory](factoryName)
    factory.build(config)
  }

  def getAvailableFactories: Set[String] = factoryMap.keys.toSet
}
