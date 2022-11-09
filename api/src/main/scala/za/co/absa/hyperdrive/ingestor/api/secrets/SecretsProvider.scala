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

package za.co.absa.hyperdrive.ingestor.api.secrets

import org.apache.commons.configuration2.Configuration

trait SecretsProvider {
  def retrieveSecret(options: Configuration): String
}

object SecretsProvider {
  val BaseKey = "secretsprovider"
  val ConfigKey = s"${BaseKey}.config"
  val ConfigProvidersKey = s"${ConfigKey}.providers"
  val ConfigDefaultProviderKey = s"${ConfigKey}.defaultprovider"
  val SecretsKey = s"${BaseKey}.secrets"
  val PerSecretProviderKey = "provider"
  val PerSecretSecretValueKey = "secretvalue"
  val PerSecretOptionsKey = "options"
  val PerProviderClassKey = "class"
}
