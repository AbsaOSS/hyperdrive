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

import org.scalatest.FlatSpec
import za.co.absa.hyperdrive.shared.configurations.ConfigurationsKeys._
import za.co.absa.hyperdrive.driver.SparkIngestor.KEY_APP_NAME

class TestCommandLineIngestionDriver extends FlatSpec {

  behavior of CommandLineIngestionDriver.getClass.getSimpleName

  it should "load all configuration" in {
    val conf = getSampleConfs
    val confPairArray = conf.map(_.productIterator.mkString("=")).toArray

    val parsedConf = CommandLineIngestionDriver.parseConfiguration(confPairArray)

    assert(conf.keys.size == parsedConf.size())
    conf.foreach {case(key,value) => assert(value == parsedConf.getString(key))}
  }

  it should "throw if any configuration is malformed" in {
    val invalidSetting = "key2="
    val invalidConfString = Array("key1=value1,value2", invalidSetting, "key3=value3")
    val throwable = intercept[IllegalArgumentException](CommandLineIngestionDriver.parseConfiguration(invalidConfString))
    assert(throwable.getMessage.toLowerCase.contains(invalidSetting))
  }

  private def getSampleConfs: Map[String,String] = {
    import KafkaStreamReaderKeys._
    Map(
      KEY_APP_NAME -> "any_name",
      KEY_TOPIC -> "any_topic",
      KEY_BROKERS -> "localhost:9092,otherhost:9093",
      KEY_KEY_PASSWORD -> "any.key.password",
      KEY_KEYSTORE_PASSWORD -> "any-keystore!!@#$% password",
      KEY_KEYSTORE_LOCATION -> "/tmp/wherever/keystore.jks",
      KEY_TRUSTSTORE_PASSWORD -> "kd9910))383(((*-+",
      KEY_TRUSTSTORE_LOCATION -> "another/place/truststore.jks",
      "key.equals.sign.in.value" -> "value1=value2"
    )
  }
}
