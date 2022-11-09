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

package za.co.absa.hyperdrive.driver.drivers.secrets.implementation.aws

import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient
import software.amazon.awssdk.services.secretsmanager.model.{GetSecretValueRequest, GetSecretValueResponse}


class DummySecretsManagerClient(secretId: String, secretString: String) extends SecretsManagerClient {
  override def serviceName(): String = ???

  override def close(): Unit = ???

  override def getSecretValue(getSecretValueRequest: GetSecretValueRequest): GetSecretValueResponse = {
    if (getSecretValueRequest.secretId() == secretId) {
      GetSecretValueResponse.builder().secretString(secretString).build()
    } else {
      throw new IllegalArgumentException(s"${getSecretValueRequest.secretId()} does not exist")
    }
  }
}
