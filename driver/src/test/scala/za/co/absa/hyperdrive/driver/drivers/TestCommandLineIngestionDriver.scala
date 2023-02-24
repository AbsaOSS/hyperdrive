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

package za.co.absa.hyperdrive.driver.drivers

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class TestCommandLineIngestionDriver extends AnyFlatSpec with Matchers {

  behavior of CommandLineIngestionDriver.getClass.getSimpleName

  it should "load all configuration" in {
    val settings = getSettings

    val config = CommandLineIngestionDriver.loadConfiguration(settings)

    config.getStringArray("reader.kafka.brokers") shouldBe Array("localhost:9092", "otherhost:9093")
    config.getString("ssl.keystore.password") shouldBe "any-keystore!!@#$% password"
    config.getString("ssl.truststore.password") shouldBe "kd9910))383(((*-+"
    config.getString("ssl.truststore.location") shouldBe "another/place/truststore.jks"
    config.getString("key.equals.sign.in.value") shouldBe "value1=value2"
    config.getLong("some.long") shouldBe 3000000000L
    config.getLong("some.interpolated.value") shouldBe 3000000000999L
    config.getString("key.with.whitespace") shouldBe "the-value"
    config.containsKey("key.without.value") shouldBe true

    val properties = config.getProperties("some.properties")
    properties.getProperty("key1") shouldBe "value1"
    properties.getProperty("key2") shouldBe "value2"
    properties.getProperty("key3") shouldBe "value3"
  }

  it should "throw if any configuration is malformed" in {
    val invalidSetting = "key2"
    val invalidConfString = Array("key1=value1,value2", invalidSetting, "key3=value3")
    val throwable = intercept[IllegalArgumentException](CommandLineIngestionDriver.loadConfiguration(invalidConfString))
    assert(throwable.getMessage.toLowerCase.contains(invalidSetting))
  }

  private def getSettings: Array[String] = {
    import za.co.absa.hyperdrive.ingestor.implementation.reader.kafka.KafkaStreamReader.KEY_BROKERS
    Array(
      s"$KEY_BROKERS=localhost:9092,otherhost:9093",
      "ssl.keystore.password=any-keystore!!@#$% password",
      "ssl.truststore.password=kd9910))383(((*-+",
      "ssl.truststore.location=another/place/truststore.jks",
      "key.equals.sign.in.value=value1=value2",
      "some.long=3000000000",
      "some.interpolated.value=${some.long}999",
      "some.properties=key1 = value1, key2=value2, key3=value3",
      " key.with.whitespace = the-value",
      "key.without.value="
    )
  }
}
