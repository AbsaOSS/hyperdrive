/*
 * Copyright 2018-2019 ABSA Group Limited
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

import java.nio.file.Files
import java.util.Properties

import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.containers.{GenericContainer, KafkaContainer, Network}
import za.co.absa.hyperdrive.testutils.SparkTestBase

case class SchemaRegistryContainer(dockerImageName: String) extends GenericContainer[SchemaRegistryContainer](dockerImageName)

/**
 * This e2e test requires a Docker installation on the executing machine.
 */
class CommandLineIngestionDriverDockerTest extends FlatSpec with Matchers with SparkTestBase with BeforeAndAfter {

  private val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  private val baseDirPath = Files.createTempDirectory("hyperdriveE2eTest")
  private val baseDir = baseDirPath.toUri
  private val checkpointDir = s"$baseDir/checkpoint"
  private val destinationDir = s"$baseDir/destination"

  private val confluentPlatformVersion = "5.3.1"
  private val schemaRegistryPort = 8081
  private val commonNetwork = Network.newNetwork()
  private val kafka = startKafka(commonNetwork)
  private val schemaRegistry = startSchemaRegistry(commonNetwork)

  private def startKafka(network: Network): KafkaContainer = {
    val kafka = new KafkaContainer(confluentPlatformVersion).withNetwork(network)
    kafka.start()
    kafka
  }

  private def startSchemaRegistry(network: Network): SchemaRegistryContainer = {
    val kafkaBrokerUrlInsideDocker = "PLAINTEXT://" + kafka.getNetworkAliases.get(0) + ":9092";
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

  private def createProducer(): KafkaProducer[Int, GenericRecord] = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer")
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "AvroProducer")
    props.put("schema.registry.url", schemaRegistryUrl)
    new KafkaProducer[Int, GenericRecord](props)
  }

  private def schemaRegistryUrl: String = s"http://${schemaRegistry.getContainerIpAddress}:${schemaRegistry.getMappedPort(schemaRegistryPort)}"

  behavior of "CommandLineIngestionDriver"

  before {
    fs.mkdirs(new Path(checkpointDir))
    fs.mkdirs(new Path(destinationDir))
  }

  it should "execute the whole pipeline" in {
    // given
    val topic = "e2etest"
    val schemaString = "{\"type\": \"record\", \"name\": \"" + topic + "\", \"fields\": [" +
      "{\"type\": \"string\", \"name\": \"field1\"}, " +
      "{\"type\": \"int\", \"name\": \"field2\"}" +
      "]}"
    val schema = new Parser().parse(schemaString)

    val producer = createProducer()
    for (i <- 0 until 50) {
      val record = new GenericData.Record(schema)
      record.put("field1", "hello")
      record.put("field2", i)
      val producerRecord = new ProducerRecord[Int, GenericRecord](topic, 1, record)
      producer.send(producerRecord)
    }

    val driverConfig = Map(
      // Pipeline settings
      "component.ingestor" -> "spark",
      "component.reader" -> "za.co.absa.hyperdrive.ingestor.implementation.reader.kafka.KafkaStreamReader",
      "component.decoder" -> "za.co.absa.hyperdrive.ingestor.implementation.decoder.avro.confluent.ConfluentAvroKafkaStreamDecoder",
      "component.manager" -> "za.co.absa.hyperdrive.ingestor.implementation.manager.checkpoint.CheckpointOffsetManager",
      "component.transformer" -> "za.co.absa.hyperdrive.ingestor.implementation.transformer.column.selection.ColumnSelectorStreamTransformer",
      "component.writer" -> "za.co.absa.hyperdrive.ingestor.implementation.writer.parquet.ParquetPartitioningStreamWriter",
      "component.finalizer" -> "za.co.absa.hyperdrive.ingestor.implementation.finalizer.noop.NoOpFinalizer",

      // Spark settings
      "ingestor.spark.app.name" -> "ingestor-app",

      // Source(Kafka) settings
      "reader.kafka.topic" -> topic,
      "reader.kafka.brokers" -> s"http://${kafka.getContainerIpAddress}:${kafka.getMappedPort(kafka.getExposedPorts.get(0))}",

      // Offset management(checkpointing) settings
      "manager.checkpoint.base.location" -> checkpointDir,

      // Format(ABRiS) settings
      "decoder.avro.schema.registry.url" -> schemaRegistryUrl,
      "decoder.avro.value.schema.id" -> "latest",
      "decoder.avro.value.schema.naming.strategy" -> "topic.name",
      "decoder.avro.schema.retention.policy" -> "RETAIN_SELECTED_COLUMN_ONLY",

      // Transformations(Enceladus) settings
      // comma separated list of columns to select
      "transformer.columns.to.select" -> "*",

      // Sink(Parquet) settings
      "writer.parquet.destination.directory" -> destinationDir
    )
    val driverConfigArray = driverConfig.map { case (key, value) => s"$key=$value" }.toArray

    // when
    CommandLineIngestionDriver.main(driverConfigArray)

    // then
    val df = spark.read.parquet(destinationDir)
    df.count shouldBe 50L
    import spark.implicits._
    df.columns should contain theSameElementsAs List("hyperdrive_date", "hyperdrive_version", "field1", "field2")
    df.select("field1").distinct()
      .map(_ (0).asInstanceOf[String]).collect() should contain theSameElementsAs List("hello")

    df.select("field2")
      .map(_ (0).asInstanceOf[Int]).collect() should contain theSameElementsAs List.range(0, 50)
  }

  after {
        fs.delete(new Path(baseDir), true)
  }
}
