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

package za.co.absa.hyperdrive.ingestor.implementation.reader.factories

import org.apache.commons.configuration2.Configuration
import org.slf4j.LoggerFactory
import za.co.absa.hyperdrive.ingestor.api.reader.{StreamReader, StreamReaderFactory}
import za.co.absa.hyperdrive.shared.utils.ClassLoaderUtils

/**
  * Abstract factory for stream readers.
  *
  * After creating a new StreamReader implementation, add the corresponding factory to "factoryMap".
  */
object StreamReaderAbstractFactory {

  private val logger = LoggerFactory.getLogger(this.getClass)
  val componentConfigKey = "component.reader"

  def build(config: Configuration): StreamReader = {

    logger.info(s"Going to load factory for configuration '$componentConfigKey'.")

    val factoryName = config.getString(componentConfigKey)
    val factory = ClassLoaderUtils.loadSingletonClassOfType[StreamReaderFactory](factoryName)
    factory.apply(config)
  }
}
