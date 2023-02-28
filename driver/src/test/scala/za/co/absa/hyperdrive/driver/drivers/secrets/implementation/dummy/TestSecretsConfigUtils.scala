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

import org.apache.commons.configuration2.BaseConfiguration
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.hyperdrive.driver.drivers.secrets.implementation.dummy.DummySecretsProvider.{DummyKey, DummySecret}
import za.co.absa.hyperdrive.driver.secrets.SecretsConfigUtils

class TestSecretsConfigUtils extends AnyFlatSpec with Matchers {
  it should "resolve secrets in the configuration" in {
    val config = new BaseConfiguration
    config.addProperty("secretsprovider.config.providers.dummysecretsmanager.class",
      "za.co.absa.hyperdrive.driver.drivers.secrets.implementation.dummy.DummySecretsProvider")
    config.addProperty("secretsprovider.secrets.mypassword.provider", "dummysecretsmanager")
    config.addProperty(s"secretsprovider.secrets.mypassword.options.${DummyKey}", "dummyValue")
    config.addProperty("service.password", "${secretsprovider.secrets.mypassword.secretvalue}")

    SecretsConfigUtils.resolveSecrets(config)

    config.getString("service.password") shouldBe s"dummyValue-${DummySecret}"
  }

  it should "use the default provider" in {
    val config = new BaseConfiguration
    config.addProperty("secretsprovider.config.providers.dummysecretsmanager.class",
      "za.co.absa.hyperdrive.driver.drivers.secrets.implementation.dummy.DummySecretsProvider")
    config.addProperty("secretsprovider.config.defaultprovider", "dummysecretsmanager")
    config.addProperty(s"secretsprovider.secrets.mypassword.options.${DummyKey}", "value")
    config.addProperty(s"secretsprovider.secrets.myotherpassword.options.${DummyKey}", "otherValue")
    config.addProperty("service.password", "${secretsprovider.secrets.mypassword.secretvalue}")
    config.addProperty("otherservice.password", "${secretsprovider.secrets.myotherpassword.secretvalue}")

    SecretsConfigUtils.resolveSecrets(config)

    config.getString("service.password") shouldBe s"value-${DummySecret}"
    config.getString("otherservice.password") shouldBe s"otherValue-${DummySecret}"
  }

  it should "throw an exception if the secrets provider was not configured" in {
    val config = new BaseConfiguration
    config.addProperty("secretsprovider.config.providers.dummysecretsmanager.class",
      "za.co.absa.hyperdrive.driver.drivers.secrets.implementation.dummy.DummySecretsProvider")
    config.addProperty("secretsprovider.config.defaultprovider", "dummysecretsmanager")
    config.addProperty("secretsprovider.secrets.mypassword.provider", "non-existing-secrets-manager")
    config.addProperty(s"secretsprovider.secrets.mypassword.options.${DummyKey}", "value")

    val result = the[NoSuchElementException] thrownBy SecretsConfigUtils.resolveSecrets(config)

    result.getMessage should include("Secrets Provider non-existing-secrets-manager does not exist")
  }

  it should "throw an exception if neither a per-secret provider nor a default provider was configured" in {
    val config = new BaseConfiguration
    config.addProperty("secretsprovider.config.providers.dummysecretsmanager.class",
      "za.co.absa.hyperdrive.driver.drivers.secrets.implementation.dummy.DummySecretsProvider")
    config.addProperty(s"secretsprovider.secrets.mypassword.options.${DummyKey}", "value")

    val result = the[NoSuchElementException] thrownBy SecretsConfigUtils.resolveSecrets(config)

    result.getMessage should include("Either secretsprovider.secrets.mypassword.provider or" +
      " secretsprovider.config.defaultprovider must be set")
  }

  it should "redact secret values from the configuration" in {
    val config = new BaseConfiguration
    config.addProperty("secretsprovider.secrets.mypassword.secretvalue", "password")
    config.addProperty("secretsprovider.secrets.mypassword.options.dummykey", "dummyvalue")
    config.addProperty("secretsprovider.secrets.myotherpassword.secretvalue", "otherpassword")

    val result = SecretsConfigUtils.getRedactedConfigurationAsMap(config)

    result should contain theSameElementsAs Map(
      "secretsprovider.secrets.mypassword.secretvalue" -> "*****",
      "secretsprovider.secrets.mypassword.options.dummykey" -> "dummyvalue",
      "secretsprovider.secrets.myotherpassword.secretvalue" -> "*****"
    )
  }
}
