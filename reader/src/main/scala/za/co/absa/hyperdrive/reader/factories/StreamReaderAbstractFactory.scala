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

package za.co.absa.hyperdrive.reader.factories

import org.apache.commons.configuration2.Configuration
import org.apache.logging.log4j.LogManager
import za.co.absa.hyperdrive.reader.StreamReaderFactory
import za.co.absa.hyperdrive.reader.factories.kafka.KafkaStreamReaderFactory

import scala.util.{Failure, Success, Try}

/**
  * Abstract factory for stream readers.
  *
  * After creating a new StreamReader implementation, add the corresponding factory to "factoryMap".
  */
object StreamReaderAbstractFactory {

  private val logger = LogManager.getLogger
  val componentConfigKey = "component.reader"

  private val factoryMap = Map[String,StreamReaderFactory](
    KafkaStreamReaderFactory.name -> KafkaStreamReaderFactory)

  def getFactory(config: Configuration): StreamReaderFactory = {

    logger.info(s"Going to load factory for configuration '$componentConfigKey'.")

    val factoryName = config.getString(componentConfigKey)

    Try(factoryMap(factoryName)) match {
      case Success(factory) => factory
      case Failure(exception) => throw new IllegalArgumentException(s"Invalid StreamReaderFactory name: '$factoryName'.", exception)
    }
  }

  def getAvailableFactories: Set[String] = factoryMap.keys.toSet
}
