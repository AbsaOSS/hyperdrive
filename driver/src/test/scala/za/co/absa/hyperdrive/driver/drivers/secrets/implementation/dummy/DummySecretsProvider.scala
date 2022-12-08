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

package za.co.absa.hyperdrive.driver.drivers.secrets.implementation.dummy

import org.apache.commons.configuration2.Configuration
import za.co.absa.hyperdrive.driver.drivers.secrets.implementation.dummy.DummySecretsProvider.{DummyKey, DummySecret}
import za.co.absa.hyperdrive.ingestor.api.PropertyMetadata
import za.co.absa.hyperdrive.ingestor.api.secrets.{SecretsProvider, SecretsProviderFactory}

class DummySecretsProvider extends SecretsProvider {
  override def retrieveSecret(options: Configuration): String =
    if (options.containsKey(DummyKey)) {
      s"${options.getString(DummyKey)}-${DummySecret}"
    } else {
      throw new IllegalArgumentException(s"Wrong options passed: ${options}. Expected key = $DummyKey")
    }
  }

object DummySecretsProvider extends SecretsProviderFactory {
  val DummyKey = "dummyKey"
  val DummySecret = "password"

  override def apply(config: Configuration): SecretsProvider = new DummySecretsProvider

  /**
   * @return a human readable name of the component.
   */
  override def getName: String = ???

  /**
   * @return a description for the component.
   */
  override def getDescription: String = ???

  /**
   * @return a map describing configuration properties for this component. The keys have to be unique to avoid
   *         name clashes with properties from other components.
   */
  override def getProperties: Map[String, PropertyMetadata] = ???
}
