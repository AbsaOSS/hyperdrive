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

package za.co.absa.hyperdrive.driver.secrets

import org.apache.commons.configuration2.Configuration
import za.co.absa.hyperdrive.ingestor.api.secrets.SecretsProviderCommonAttributes._
import za.co.absa.hyperdrive.ingestor.api.utils.ConfigUtils

import scala.util.Try

object SecretsConfigUtils {
  private val RedactedSecret = "*****"

  def resolveSecrets(config: Configuration): Unit = {
    val secretsProviders = SecretsProviderAbstractFactory.build(config)
    val defaultProvider = Option(config.getString(configDefaultProviderKey))
    val secretDescriptors = ConfigUtils.getSubsets(config, secretsKey)
    secretDescriptors.foreach { case (name, subsetConfig) =>
      val provider = Option(subsetConfig.getString(perSecretProviderKey))
        .getOrElse(defaultProvider
          .getOrElse(
            throw new NoSuchElementException(s"Either ${secretsKey}.$name.${perSecretProviderKey} or " +
              s"$configDefaultProviderKey must be set")
          )
        )
      val secretsProvider = Try(secretsProviders(provider)).getOrElse(throw new NoSuchElementException(
        s"Secrets Provider $provider does not exist. Existing providers: ${secretsProviders.keys}")
      )

      val secret = secretsProvider.retrieveSecret(subsetConfig.subset(perSecretOptionsKey))
      config.addProperty(s"${secretsKey}.$name.${perSecretSecretValueKey}", secret)
    }
  }

  def getRedactedConfigurationAsMap(configuration: Configuration): Map[String, AnyRef] = {
    import scala.collection.JavaConverters._
    configuration
      .getKeys
      .asScala
      .map { key =>
        val property = if (key.contains(perSecretSecretValueKey)) {
          RedactedSecret
        } else {
          configuration.getProperty(key)
        }
        key -> property
      }
      .toMap
  }
}
