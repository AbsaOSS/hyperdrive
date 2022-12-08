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

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.configuration2.Configuration
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest
import za.co.absa.hyperdrive.driver.secrets.implementation.aws.AwsSecretsManagerSecretsProvider.{Base64Encoding, Encoding, ReadAsMap, Key, SecretName}
import za.co.absa.hyperdrive.ingestor.api.secrets.{SecretsProvider, SecretsProviderFactory}
import za.co.absa.hyperdrive.ingestor.api.utils.ConfigUtils

import java.util
import java.util.Base64

class AwsSecretsManagerSecretsProvider(secretsManagerClient: SecretsManagerClient) extends SecretsProvider {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val objectMapper = new ObjectMapper()
  override def retrieveSecret(options: Configuration): String = {
    val secretName = ConfigUtils.getOrThrow(SecretName, options)
    val secretString = getSecretValue(secretName).secretString()

    val readAsMap = ConfigUtils.getOptionalBoolean(ReadAsMap, options).getOrElse(true)
    val secret = if (readAsMap) {
      val secretKey = ConfigUtils.getOrThrow(Key, options)
      import scala.collection.JavaConverters._
      val secretMap = objectMapper.readValue(secretString, classOf[util.HashMap[String, String]]).asScala
      secretMap(secretKey)
    } else {
      secretString
    }

    Option(options.getString(Encoding)) match {
      case Some(encoding) => if (encoding == Base64Encoding) {
        new String(Base64.getDecoder.decode(secret), "UTF-8")
      } else {
        logger.warn(s"Ignoring unknown encoding: ${encoding}")
        secret
      }
      case None => secret
    }
  }

  private def getSecretValue(secretName: String) = {
    val request = GetSecretValueRequest.builder()
      .secretId(secretName)
      .build()
    secretsManagerClient.getSecretValue(request)
  }
}

object AwsSecretsManagerSecretsProvider extends SecretsProviderFactory with AwsSecretsManagerSecretsProviderAttributes {

  override def apply(config: Configuration): SecretsProvider =
    new AwsSecretsManagerSecretsProvider(SecretsManagerClient.builder().build())
}
