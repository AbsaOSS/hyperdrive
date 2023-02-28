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

import org.apache.commons.configuration2.BaseConfiguration
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.hyperdrive.driver.secrets.implementation.aws.AwsSecretsManagerSecretsProvider

class TestAwsSecretsManagerSecretsProvider extends AnyFlatSpec with Matchers {

  it should "retrieve a secret value in a map" in {
    val options = new BaseConfiguration
    options.addProperty(AwsSecretsManagerSecretsProvider.SecretName, "secret-id")
    options.addProperty(AwsSecretsManagerSecretsProvider.Key, "secret-key")

    val secretString = raw"""{"secret-key": "password"}"""
    val underTest = new AwsSecretsManagerSecretsProvider(new DummySecretsManagerClient("secret-id", secretString))

    val result = underTest.retrieveSecret(options)

    result shouldBe "password"
  }

  it should "retrieve a secret value in plaintext" in {
    val options = new BaseConfiguration
    options.addProperty(AwsSecretsManagerSecretsProvider.SecretName, "secret-id")
    options.addProperty(AwsSecretsManagerSecretsProvider.ReadAsMap, "false")

    val secretString = "password"
    val underTest = new AwsSecretsManagerSecretsProvider(new DummySecretsManagerClient("secret-id", secretString))

    val result = underTest.retrieveSecret(options)

    result shouldBe "password"
  }

  it should "retrieve a secret value and decode it" in {
    val options = new BaseConfiguration
    options.addProperty(AwsSecretsManagerSecretsProvider.SecretName, "secret-id")
    options.addProperty(AwsSecretsManagerSecretsProvider.Key, "secret-key")
    options.addProperty(AwsSecretsManagerSecretsProvider.Encoding, AwsSecretsManagerSecretsProvider.Base64Encoding)

    val secretString = raw"""{"secret-key": "cGFzc3dvcmQ="}"""
    val underTest = new AwsSecretsManagerSecretsProvider(new DummySecretsManagerClient("secret-id", secretString))

    val result = underTest.retrieveSecret(options)

    result shouldBe "password"
  }

  it should "fail if the secret name is not provided in the options" in {
    val underTest = new AwsSecretsManagerSecretsProvider(new DummySecretsManagerClient("secret-id", "secret"))

    val exception = the[IllegalArgumentException] thrownBy underTest.retrieveSecret(new BaseConfiguration)

    exception.getMessage should include(AwsSecretsManagerSecretsProvider.SecretName)
  }

  it should "fail if it should be read as a map, but the secret value is not a map" in {
    val options = new BaseConfiguration
    options.addProperty(AwsSecretsManagerSecretsProvider.SecretName, "secret-id")
    options.addProperty(AwsSecretsManagerSecretsProvider.Key, "secret-key")
    val underTest = new AwsSecretsManagerSecretsProvider(new DummySecretsManagerClient("secret-id", "secret-is-not-a-map"))

    val exception = the[Exception] thrownBy underTest.retrieveSecret(options)

    exception.getMessage should include("secret-is-not-a-map")
  }

  it should "fail if it should be read as a map, but the secret key is not configured" in {
    val options = new BaseConfiguration
    options.addProperty(AwsSecretsManagerSecretsProvider.SecretName, "secret-id")
    val underTest = new AwsSecretsManagerSecretsProvider(new DummySecretsManagerClient("secret-id", "secret"))

    val exception = the[Exception] thrownBy underTest.retrieveSecret(options)

    exception.getMessage should include(AwsSecretsManagerSecretsProvider.Key)
  }

  it should "fail if it should be read as a map, but the secret key is not available in the returned map" in {
    val options = new BaseConfiguration
    options.addProperty(AwsSecretsManagerSecretsProvider.SecretName, "secret-id")
    options.addProperty(AwsSecretsManagerSecretsProvider.Key, "not-the-secret-key")
    val secretString = raw"""{"secret-key": "password"}"""
    val underTest = new AwsSecretsManagerSecretsProvider(new DummySecretsManagerClient("secret-id", secretString))

    val exception = the[NoSuchElementException] thrownBy underTest.retrieveSecret(options)

    exception.getMessage should include("not-the-secret-key")
  }
}


