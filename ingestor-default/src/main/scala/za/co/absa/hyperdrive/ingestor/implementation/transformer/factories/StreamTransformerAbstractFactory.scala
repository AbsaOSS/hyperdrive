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

package za.co.absa.hyperdrive.ingestor.implementation.transformer.factories

import org.apache.commons.configuration2.Configuration
import org.apache.logging.log4j.LogManager

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}
import za.co.absa.hyperdrive.ingestor.api.transformer.{StreamTransformer, StreamTransformerFactory}
import za.co.absa.hyperdrive.ingestor.api.transformer.StreamTransformerFactory._
import za.co.absa.hyperdrive.ingestor.api.utils.ConfigUtils
import za.co.absa.hyperdrive.shared.utils.ClassLoaderUtils


/**
 * Abstract factory for stream transformers.
 *
 * After creating a new StreamTransformer implementation, add the corresponding factory to "factoryMap" inside this class.
 */
object StreamTransformerAbstractFactory {

  private val logger = LogManager.getLogger

  /**
   * For each transformer, the configuration is assumed to contain property keys according to the following example
   * component.transformer.id.{order}={transformer-id} (Required)
   * component.transformer.class.{transformer-id}=some.fully.qualified.class.name (Required)
   * transformer.{transformer-id}.some.custom.key=some-custom-value (Optional)
   *
   * {order} must be an integer value, and must be unique to avoid name clashes among the property keys
   * {transformer-id} may be any string, but must be unique to avoid name clashes among the property keys
   */
  def build(config: Configuration): Seq[StreamTransformer] = {

    logger.info(s"Going to load factory for transformer configurations.")

    validateConfiguration(config)

    val orderedTransformerIds = config.getKeys(IdsKeyPrefix).asScala.toList
      .map(key => key.replace(s"$IdsKeyPrefix.", "").toInt -> config.getString(key))
      .sortBy { case (order, _) => order }
      .map { case (_, id) => id }

    val transformerClassNames = orderedTransformerIds.map(id => id -> config.getString(s"$ClassKeyPrefix.$id"))

    transformerClassNames
      .map { case (id, className) => id -> ClassLoaderUtils.loadSingletonClassOfType[StreamTransformerFactory](className) }
      .map { case (id, factory) => factory -> ConfigUtils.copyAndMapConfig(config, config.subset(s"$TransformerKeyPrefix.$id"), factory.getMappingFromRetainedGlobalConfigToLocalConfig(config)) }
      .map { case (factory, configTry) => configTry match {
        case Failure(exception) => throw exception
        case Success(value) => factory -> value
      }
      }
      .map { case (factory, config) => factory.apply(config) }
  }

  private def validateConfiguration(config: Configuration): Unit = {
    val keys = config.getKeys(IdsKeyPrefix).asScala.toList

    val invalidTransformerKeys = keys
      .map(key => key -> key.replace(s"$IdsKeyPrefix.", ""))
      .map { case (key, order) => key -> Try(order.toInt) }
      .filter { case (_, orderAsInt) => orderAsInt.isFailure }
      .map { case (key, _) => key }
    if (invalidTransformerKeys.nonEmpty) {
      throw new IllegalArgumentException(s"Transformer Id Keys with non-numeric order encountered. $invalidTransformerKeys")
    }

    val transformerIds = keys.map(key => config.getString(key))
    if (transformerIds.toSet.size != transformerIds.size) {
      throw new IllegalArgumentException(s"Non-unique transformer ids encountered. $transformerIds")
    }

    val missingClassKeys = transformerIds
      .map(id => s"$ClassKeyPrefix.$id")
      .filter(classKey => !config.containsKey(classKey))

    if (missingClassKeys.nonEmpty) {
      throw new IllegalArgumentException(s"Did not find expected configuration properties $missingClassKeys")
    }
  }
}
