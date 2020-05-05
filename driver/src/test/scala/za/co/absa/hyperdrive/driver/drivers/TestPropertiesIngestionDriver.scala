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

import org.scalatest.{FlatSpec, Matchers}
import za.co.absa.hyperdrive.driver.SparkIngestor.KEY_APP_NAME
import za.co.absa.hyperdrive.shared.configurations.ConfigurationsKeys._

class TestPropertiesIngestionDriver extends FlatSpec with Matchers {

  behavior of PropertiesIngestionDriver.getClass.getSimpleName

  it should "load all configuration" in {
    val configurationFile = getClass.getClassLoader.getResource("ingestion.properties").getPath

    val config = PropertiesIngestionDriver.loadConfiguration(configurationFile)

    config.getString("ingestor.spark.app.name") shouldBe "any_name"
    config.getStringArray("reader.kafka.brokers") shouldBe Array("localhost:9092", "otherhost:9093")
    config.getString("ssl.keystore.password") shouldBe "any-keystore!!@#$% password"
    config.getString("ssl.truststore.password") shouldBe "kd9910))383(((*-+"
    config.getString("ssl.truststore.location") shouldBe "another/place/truststore.jks"
    config.getString("key.equals.sign.in.value") shouldBe "value1=value2"
    config.getLong("some.long") shouldBe 3000000000L
    config.getLong("some.interpolated.value") shouldBe 3000000000999L

    val properties = config.getProperties("some.properties")
    properties.getProperty("key1") shouldBe "value1"
    properties.getProperty("key2") shouldBe "value2"
    properties.getProperty("key3") shouldBe "value3"
  }
}
