/*
 *  Copyright 2019 ABSA Group Limited
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package za.co.absa.hyperdrive.ingestor.implementation.reader.factories.kafka

import org.apache.commons.configuration2.DynamicCombinedConfiguration
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler
import org.scalatest.{BeforeAndAfterEach, FlatSpec}
import za.co.absa.hyperdrive.ingestor.implementation.reader.kafka.KafkaStreamReader
import za.co.absa.hyperdrive.shared.configurations.ConfigurationsKeys.KafkaStreamReaderKeys._

class TestKafkaStreamReaderFactory extends FlatSpec with BeforeAndAfterEach {

  private val configStub  = new DynamicCombinedConfiguration()

  private val topic = "topic"
  private val brokers = "PLAINTEXT://broker1:9092,PROTOCOL://broker2:9093"
  private val securityProtocol = "SSL"
  private val truststoreLocation = "/tmp/truststore/wherever"
  private val truststorePassword = "our-little-secret"
  private val keystoreLocation = "/tmp/keystore/wherever"
  private val keystorePassword = "we-are-very-secretive"
  private val keyPassword = "arent-we?"

  private val securityConfsMap = Map[String,String](
    KEY_SECURITY_PROTOCOL -> securityProtocol,
    KEY_KEYSTORE_LOCATION -> keystoreLocation,
    KEY_KEYSTORE_PASSWORD -> keystorePassword,
    KEY_TRUSTSTORE_LOCATION -> truststoreLocation,
    KEY_TRUSTSTORE_PASSWORD -> truststorePassword,
    KEY_KEY_PASSWORD -> keyPassword)

  behavior of KafkaStreamReaderFactory.getClass.getSimpleName

  override def beforeEach(): Unit = {
    configStub.clear()
    configStub.setListDelimiterHandler(new DefaultListDelimiterHandler(','))
  }

  it should "throw if topic is blank" in {
    stubBrokers() // setup brokers
    stubSecurity() // setup security

    val throwable = intercept[IllegalArgumentException](KafkaStreamReaderFactory.build(configStub))
    assert(throwable.getMessage.toLowerCase.contains("topic")) // makes sure the error message mentions topic as the source
  }

  it should "throw if brokers are blank" in {
    stubTopic() // setup brokers
    stubSecurity() // setup security

    val throwable = intercept[IllegalArgumentException](KafkaStreamReaderFactory.build(configStub))
    assert(throwable.getMessage.toLowerCase.contains("brokers")) // makes sure the error message mentions topic as the source
  }

  it should "not throw if extra configurations are absent" in {
    stubTopic()
    stubBrokers()

    val kafkaStreamReader: KafkaStreamReader = KafkaStreamReaderFactory.build(configStub).asInstanceOf[KafkaStreamReader]
    assert(topic == kafkaStreamReader.topic)
    assert(brokers == kafkaStreamReader.brokers)
    assert(kafkaStreamReader.extraConfs.isEmpty)
  }

  it should "set extra configurations if they are specified" in {
    stubTopic()
    stubBrokers()
    stubSecurity()

    val kafkaStreamReader: KafkaStreamReader = KafkaStreamReaderFactory.build(configStub).asInstanceOf[KafkaStreamReader]
    assert(topic == kafkaStreamReader.topic)
    assert(brokers == kafkaStreamReader.brokers)

    import kafkaStreamReader.extraConfs
    securityConfsMap.foreach {case(key,value) => assert(value == extraConfs(removeOptionalComponentFromKey(key)))}
  }

  private def stubProperty(key: String, value: String): Unit = configStub.addProperty(key, value)

  private def stubTopic(): Unit = stubProperty(KEY_TOPIC, topic)

  private def stubBrokers(): Unit = stubProperty(KEY_BROKERS, brokers)

  private def stubSecurity(keysToIgnore: Set[String] = Set[String]()): Unit = {
    securityConfsMap
      .filterKeys(!keysToIgnore.contains(_))
      .foreach {case(key,value) => stubProperty(key, value)}
  }

  private def removeComponentFromKey(key: String): String = key.replaceAll(s"$rootComponentConfKey.", "")

  private def removeOptionalComponentFromKey(key: String): String = key.replaceAll(s"$rootFactoryOptionalConfKey.", "")
}
