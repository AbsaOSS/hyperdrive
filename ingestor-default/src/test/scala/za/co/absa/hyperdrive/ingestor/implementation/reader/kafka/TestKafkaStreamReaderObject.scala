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

package za.co.absa.hyperdrive.ingestor.implementation.reader.kafka

import org.apache.commons.configuration2.DynamicCombinedConfiguration
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import za.co.absa.commons.io.TempDirectory
import za.co.absa.hyperdrive.ingestor.api.writer.StreamWriterCommonAttributes
import za.co.absa.hyperdrive.ingestor.implementation.reader.kafka.KafkaStreamReader.{KEY_TOPIC, KEY_BROKERS}

class TestKafkaStreamReaderObject extends AnyFlatSpec with BeforeAndAfterEach {
  private val rootFactoryOptionalConfKey = KafkaStreamReader.getExtraConfigurationPrefix.get
  private val rootFactoryOptionalKafkaKey = s"$rootFactoryOptionalConfKey.kafka"
  private val KEY_SECURITY_PROTOCOL = s"$rootFactoryOptionalKafkaKey.security.protocol"
  private val KEY_TRUSTSTORE_LOCATION = s"$rootFactoryOptionalKafkaKey.ssl.truststore.location"
  private val KEY_TRUSTSTORE_PASSWORD = s"$rootFactoryOptionalKafkaKey.ssl.truststore.password"
  private val KEY_KEYSTORE_LOCATION = s"$rootFactoryOptionalKafkaKey.ssl.keystore.location"
  private val KEY_KEYSTORE_PASSWORD = s"$rootFactoryOptionalKafkaKey.ssl.keystore.password"
  private val KEY_KEY_PASSWORD = s"$rootFactoryOptionalKafkaKey.ssl.key.password"

  private val configStub  = new DynamicCombinedConfiguration()

  private val topic = "topic"
  private val brokers = "PLAINTEXT://broker1:9092,PROTOCOL://broker2:9093"
  private val securityProtocol = "SSL"
  private val truststoreLocation = "/tmp/truststore/wherever"
  private val truststorePassword = "our-little-secret"
  private val keystoreLocation = "/tmp/keystore/wherever"
  private val keystorePassword = "we-are-very-secretive"
  private val keyPassword = "arent-we?"
  private val checkpointDir = {
    val dir = TempDirectory()
    dir.deleteOnExit()
    dir
  }

  private val extraOptions = Map[String,String](
    KEY_SECURITY_PROTOCOL -> securityProtocol,
    KEY_KEYSTORE_LOCATION -> keystoreLocation,
    KEY_KEYSTORE_PASSWORD -> keystorePassword,
    KEY_TRUSTSTORE_LOCATION -> truststoreLocation,
    KEY_TRUSTSTORE_PASSWORD -> truststorePassword,
    KEY_KEY_PASSWORD -> keyPassword,
    s"$rootFactoryOptionalConfKey.failOnDataLoss" -> "false")

  behavior of KafkaStreamReader.getClass.getSimpleName

  override def beforeEach(): Unit = {
    configStub.clear()
    configStub.setListDelimiterHandler(new DefaultListDelimiterHandler(','))
  }

  it should "throw if topic is blank" in {
    stubBrokers()
    stubSecurity()
    stubCheckpointLocation()

    val throwable = intercept[IllegalArgumentException](KafkaStreamReader(configStub))
    assert(throwable.getMessage.toLowerCase.contains("topic"))
  }

  it should "throw if brokers are blank" in {
    stubTopic()
    stubSecurity()
    stubCheckpointLocation()

    val throwable = intercept[IllegalArgumentException](KafkaStreamReader(configStub))
    assert(throwable.getMessage.toLowerCase.contains("brokers"))
  }

  it should "throw on blank checkpoint location" in {
    stubBrokers()
    stubTopic()

    val throwable = intercept[IllegalArgumentException](KafkaStreamReader(configStub))
    assert(throwable.getMessage.toLowerCase.contains(StreamWriterCommonAttributes.keyCheckpointBaseLocation))
  }

  it should "not throw if extra configurations are absent" in {
    stubTopic()
    stubBrokers()
    stubCheckpointLocation()

    val kafkaStreamReader: KafkaStreamReader = KafkaStreamReader(configStub).asInstanceOf[KafkaStreamReader]
    assert(topic == kafkaStreamReader.topic)
    assert(brokers == kafkaStreamReader.brokers)
    assert(kafkaStreamReader.extraConfs.isEmpty)
  }

  it should "set extra configurations if they are specified" in {
    stubTopic()
    stubBrokers()
    stubSecurity()
    stubCheckpointLocation()

    val kafkaStreamReader: KafkaStreamReader = KafkaStreamReader(configStub).asInstanceOf[KafkaStreamReader]
    assert(topic == kafkaStreamReader.topic)
    assert(brokers == kafkaStreamReader.brokers)

    import kafkaStreamReader.extraConfs
    extraOptions.foreach {case(key,value) => assert(value == extraConfs(removeOptionalComponentFromKey(key)))}
  }

  private def stubProperty(key: String, value: String): Unit = configStub.addProperty(key, value)

  private def stubTopic(): Unit = stubProperty(KEY_TOPIC, topic)

  private def stubBrokers(): Unit = stubProperty(KEY_BROKERS, brokers)

  private def stubCheckpointLocation(): Unit = stubProperty(StreamWriterCommonAttributes.keyCheckpointBaseLocation, checkpointDir.path.toAbsolutePath.toString)

  private def stubSecurity(keysToIgnore: Set[String] = Set[String]()): Unit = {
    extraOptions
      .filterKeys(!keysToIgnore.contains(_))
      .foreach {case(key,value) => stubProperty(key, value)}
  }

  private def removeOptionalComponentFromKey(key: String): String =
    key.replaceAll(s"$rootFactoryOptionalConfKey.", "")
}
