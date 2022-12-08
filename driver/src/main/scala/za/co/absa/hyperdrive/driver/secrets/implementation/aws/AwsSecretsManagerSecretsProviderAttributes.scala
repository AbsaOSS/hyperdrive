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

package za.co.absa.hyperdrive.driver.secrets.implementation.aws

import za.co.absa.hyperdrive.ingestor.api.secrets.SecretsProviderCommonAttributes._
import za.co.absa.hyperdrive.ingestor.api.{HasComponentAttributes, PropertyMetadata}

trait AwsSecretsManagerSecretsProviderAttributes extends HasComponentAttributes {
  val SecretName = "secretname"
  val ReadAsMap = "readasmap"
  val Key = "key"
  val Encoding = "encoding"
  val Base64Encoding = "base64"

  /**
   * @return a human readable name of the component.
   */
  override def getName: String =  "AWS Secrets Manager Secrets Provider"

  /**
   * @return a description for the component.
   */
  override def getDescription: String = "A Secrets Provider implementation, which retrieves secrets from AWS Secrets Manager"

  /**
   * @return a map describing configuration properties for this component. The keys have to be unique to avoid
   *         name clashes with properties from other components.
   */
  override def getProperties: Map[String, PropertyMetadata] = Map(
    s"${configProvidersKey}.<provider-id>.${perProviderClassKey}" -> PropertyMetadata(
      "Secrets Provider class",
      Some("The fully qualified class name of the secrets provider. <provider-id> is an arbitrary string." +
        " Multiple secrets providers can be configured by supplying multiple <provider-id>s"),
      required = true
    ),
    s"${configDefaultProviderKey}" -> PropertyMetadata(
      "Default secrets provider",
      Some("The <provider-id> of the secrets provider to be used by default"),
      required = false
    ),
    s"${secretsKey}.<secret-id>.${perSecretOptionsKey}.${SecretName}" -> PropertyMetadata(
      "Secret Name",
      Some("The Secret name of the secret in AWS Secrets Manager. <secret-id> is an arbitrary string." +
        " Multiple secrets can be configured by supplying multiple <secret-id>s"),
      required = true
    ),
    s"${secretsKey}.<secret-id>.${perSecretOptionsKey}.${perSecretProviderKey}" -> PropertyMetadata(
      "Secrets Provider",
      Some("The <provider-id> of the secrets provider to be used for this specific secret."),
      required = true
    ),
    s"${secretsKey}.<secret-id>.${perSecretOptionsKey}.${ReadAsMap}" -> PropertyMetadata(
      "Read as map",
      Some("Set to true if the secret should be interpreted as a json map, set to false if the value should be read as is." +
        "Default: true"),
      required = false
    ),
    s"${secretsKey}.<secret-id>.${perSecretOptionsKey}.${Key}" -> PropertyMetadata(
      "Map Key",
      Some("If the secret should be read as a map, specify the key whose value should be extracted as the secret"),
      required = false
    ),
    s"${secretsKey}.<secret-id>.${perSecretOptionsKey}.${Encoding}" -> PropertyMetadata(
      "Encoding",
      Some(s"Decodes the secret. Valid values: `${Base64Encoding}`."),
      required = false
    )
  )
}
