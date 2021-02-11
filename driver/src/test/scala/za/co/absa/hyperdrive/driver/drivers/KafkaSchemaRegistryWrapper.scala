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

import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.logging.log4j.LogManager
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.containers.{GenericContainer, KafkaContainer, Network}
import org.testcontainers.utility.DockerImageName

case class SchemaRegistryContainer(dockerImageName: String) extends GenericContainer[SchemaRegistryContainer](dockerImageName)

class KafkaSchemaRegistryWrapper {
  private val logger = LogManager.getLogger

  private val confluentPlatformVersion = "5.3.1" // should be same as kafka.avro.serializer.version property in pom file
  private val schemaRegistryPort = 8081
  private val commonNetwork = Network.newNetwork()
  val kafka: KafkaContainer = startKafka(commonNetwork)
  private val schemaRegistry = startSchemaRegistry(commonNetwork)

  logger.info(s"Created network with id ${commonNetwork.getId}")

  private def startKafka(network: Network): KafkaContainer = {
    val kafka = new KafkaContainer(DockerImageName.parse(s"confluentinc/cp-kafka:$confluentPlatformVersion"))
      .withNetwork(network)
    kafka.start()
    kafka
  }

  private def startSchemaRegistry(network: Network): SchemaRegistryContainer = {
    val kafkaBrokerUrlInsideDocker = "PLAINTEXT://" + kafka.getNetworkAliases.get(0) + ":9092"
    val schemaRegistry =
      SchemaRegistryContainer(s"confluentinc/cp-schema-registry:$confluentPlatformVersion")
        .withExposedPorts(schemaRegistryPort)
        .withNetwork(network)
        .withEnv("SCHEMA_REGISTRY_HOST_NAME", "localhost") // loopback to the container
        .withEnv("SCHEMA_REGISTRY_LISTENERS", s"http://0.0.0.0:$schemaRegistryPort") // loopback to the container
        .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", kafkaBrokerUrlInsideDocker)
        .waitingFor(Wait.forHttp("/subjects").forStatusCode(200))
    schemaRegistry.start()
    schemaRegistry
  }

  def kafkaUrl: String = s"http://${kafka.getContainerIpAddress}:${kafka.getMappedPort(kafka.getExposedPorts.get(0))}"

  def schemaRegistryUrl: String = s"http://${schemaRegistry.getContainerIpAddress}:${schemaRegistry.getMappedPort(schemaRegistryPort)}"

  def createProducer[K, V](props: Properties): KafkaProducer[K, V] = {
    props.put("schema.registry.url", schemaRegistryUrl)
    new KafkaProducer[K, V](props)
  }

  def createConsumer[K, V](props: Properties): KafkaConsumer[K, V] = {
    props.put("schema.registry.url", schemaRegistryUrl)
    new KafkaConsumer[K, V](props)
  }
}
