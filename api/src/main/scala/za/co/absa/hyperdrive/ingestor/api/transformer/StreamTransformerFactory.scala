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

package za.co.absa.hyperdrive.ingestor.api.transformer

import org.apache.commons.configuration2.Configuration
import za.co.absa.hyperdrive.ingestor.api.ComponentFactory

trait StreamTransformerFactory extends ComponentFactory[StreamTransformer] {
  /**
   * In the apply-method, only the subset of keys with the prefix of the transformer is available. E.g. the property
   * reader.kafka.topic would not be available to the transformer. If any such key from the global config should be
   * retained and available to the transformer, it should be specified in this method.
   * To avoid key clashes, the global key may be mapped. If the global config key should not be renamed, key and value
   * of the map entry should be the same.
   * @param globalConfig The global configuration, containing all keys passed to the program
   * @return a mapping for each key in the global configuration to an arbitrary key which will be available in the
   *         configuration passed in by the apply method.
   */
  def getMappingFromRetainedGlobalConfigToLocalConfig(globalConfig: Configuration): Map[String, String] = Map()
}

object StreamTransformerFactory {
  val IdsKeyPrefix = "component.transformer.id"
  val ClassKeyPrefix = "component.transformer.class"
  val TransformerKeyPrefix = "transformer"
}
