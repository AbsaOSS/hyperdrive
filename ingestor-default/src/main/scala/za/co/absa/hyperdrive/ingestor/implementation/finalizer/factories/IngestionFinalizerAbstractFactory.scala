/*
 * Copyright 2018-2019 ABSA Group Limited
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

package za.co.absa.hyperdrive.ingestor.implementation.finalizer.factories

import org.apache.commons.configuration2.Configuration
import org.apache.logging.log4j.LogManager
import za.co.absa.hyperdrive.ingestor.api.finalizer.{IngestionFinalizer, IngestionFinalizerFactory}
import za.co.absa.hyperdrive.shared.utils.ClassLoaderUtils

object IngestionFinalizerAbstractFactory {

  private val logger = LogManager.getLogger

  val componentConfigKey = "component.finalizer"

  def build(config: Configuration): IngestionFinalizer = {

    logger.info(s"Going to load factory for configuration '$componentConfigKey'.")

    val factoryName = config.getString(componentConfigKey)
    val factory = ClassLoaderUtils.loadSingletonClassOfType[IngestionFinalizerFactory](factoryName)
    factory.apply(config)
  }
}
