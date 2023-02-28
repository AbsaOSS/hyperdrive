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

package za.co.absa.hyperdrive.ingestor.implementation.transformer.deduplicate.kafka

import java.time.Duration

import org.apache.commons.configuration2.DynamicCombinedConfiguration
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.hyperdrive.ingestor.api.writer.StreamWriterCommonAttributes
import za.co.absa.hyperdrive.ingestor.implementation.reader.kafka.KafkaStreamReader
import za.co.absa.hyperdrive.ingestor.implementation.transformer.avro.confluent.{ConfluentAvroDecodingTransformer, ConfluentAvroEncodingTransformer}
import za.co.absa.hyperdrive.ingestor.implementation.writer.kafka.KafkaStreamWriter

class TestDeduplicateKafkaSinkTransformerObject extends AnyFlatSpec with Matchers {
  behavior of DeduplicateKafkaSinkTransformer.getClass.getSimpleName

  private val decoderPrefix = "deduplicateKafkaSinkTransformer.decoder" // copied from DeduplicateKafkaSinkTransformer
  private val encoderPrefix = "deduplicateKafkaSinkTransformer.encoder" // copied from DeduplicateKafkaSinkTransformer

  private val dummySourceRegistry = "http://sourceRegistry:8081"
  private val dummyDestinationRegistry = "http://destinationRegistry:8081"

  "apply" should "create a DeduplicateKafkaSinkTransformer" in {
    // given
    val config = getLocalConfig()

    // when
    val transformer = DeduplicateKafkaSinkTransformer(config).asInstanceOf[DeduplicateKafkaSinkTransformer]

    // then
    transformer.readerTopic shouldBe "readerTopic"
    transformer.readerBrokers shouldBe "http://readerBrokers:9092"
    transformer.readerExtraOptions should contain theSameElementsAs Map(
      "security.protocol" -> "SASL_PLAINTEXT"
    )
    transformer.decoderSchemaRegistryConfig("schema.registry.url") shouldBe dummySourceRegistry
    transformer.decoderSchemaRegistryConfig("some.extra.config") shouldBe "someDecoderExtraConfig"
    transformer.writerTopic shouldBe "writerTopic"
    transformer.writerBrokers shouldBe "http://writerBrokers:9092"
    transformer.writerExtraOptions shouldBe Map(
      "sasl.mechanism" -> "GSSAPI"
    )
    transformer.encoderSchemaRegistryConfig("schema.registry.url") shouldBe dummyDestinationRegistry
    transformer.encoderSchemaRegistryConfig("some.extra.config") shouldBe "someEncoderExtraConfig"

    transformer.checkpointLocation shouldBe "/tmp/checkpoint"
    transformer.sourceIdColumnNames should contain theSameElementsInOrderAs Seq("offset", "partition")
    transformer.destinationIdColumnNames should contain theSameElementsInOrderAs Seq("value.hyperdrive_id.source_offset", "value.hyperdrive_id.source_partition")
    transformer.kafkaConsumerTimeout shouldBe Duration.ofSeconds(5L)
  }

  it should "throw an exception if source id columns and destination id columns have different size" in {
    val config = getLocalConfig()
    config.setProperty(DeduplicateKafkaSinkTransformer.DestinationIdColumns, "value.hyperdrive_id")

    val ex = the[IllegalArgumentException] thrownBy DeduplicateKafkaSinkTransformer(config)

    ex.getMessage should include ("The size of source id column names doesn't match")
  }

  it should "use the default value for kafka consumer timeout if not provided" in {
    // given
    val config = getLocalConfig()
    config.clearProperty(DeduplicateKafkaSinkTransformer.KafkaConsumerTimeout)

    // when
    val transformer = DeduplicateKafkaSinkTransformer(config).asInstanceOf[DeduplicateKafkaSinkTransformer]

    // then
    transformer.kafkaConsumerTimeout shouldBe Duration.ofSeconds(120L)
  }

  it should "throw an exception if the kafka reader config is missing" in {
    val config = getLocalConfig()
    config.clearProperty(KafkaStreamReader.KEY_TOPIC)

    val exception = the[Exception] thrownBy DeduplicateKafkaSinkTransformer(config)

    exception.getMessage should include(KafkaStreamReader.KEY_TOPIC)
  }

  it should "throw an exception if the kafka writer config is missing" in {
    val config = getLocalConfig()
    config.clearProperty(KafkaStreamWriter.KEY_TOPIC)

    val exception = the[Exception] thrownBy DeduplicateKafkaSinkTransformer(config)

    exception.getMessage should include(KafkaStreamWriter.KEY_TOPIC)
  }

  it should "throw an exception if the reader schema registry config is missing" in {
    val config = getLocalConfig()
    import scala.collection.JavaConverters._
    config.getKeys(decoderPrefix).asScala.foreach(config.clearProperty)

    val exception = the[Exception] thrownBy DeduplicateKafkaSinkTransformer(config)

    exception.getMessage should include("schema.registry.url")
  }

  it should "throw an exception if the writer schema registry config is missing" in {
    val config = getLocalConfig()
    import scala.collection.JavaConverters._
    config.getKeys(encoderPrefix).asScala.foreach(config.clearProperty)

    val exception = the[Exception] thrownBy DeduplicateKafkaSinkTransformer(config)

    exception.getMessage should include("schema.registry.url")
  }


  "getMappingFromRetainedGlobalConfigToLocalConfig" should "return the local config mapping" in {
    // given
    val config = getEmptyConfiguration
    config.addProperty("reader.option.kafka.option1", "value1")
    config.addProperty("reader.option.kafka.option2", "value2")
    config.addProperty("component.transformer.id.0", "decoder")
    config.addProperty("component.transformer.class.decoder", classOf[ConfluentAvroDecodingTransformer].getCanonicalName)
    config.addProperty(s"transformer.decoder.${ConfluentAvroDecodingTransformer.KEY_SCHEMA_REGISTRY_URL}", dummySourceRegistry)
    config.addProperty(s"transformer.decoder.schema.registry.options.some.key", "schema.registry.extra.value")

    config.addProperty("component.transformer.id.1", "encoder")
    config.addProperty("component.transformer.class.encoder", classOf[ConfluentAvroEncodingTransformer].getCanonicalName)
    config.addProperty(s"transformer.encoder.${ConfluentAvroEncodingTransformer.KEY_SCHEMA_REGISTRY_URL}", dummyDestinationRegistry)

    config.addProperty("writer.kafka.option.option3", "value3")
    // when
    val mapping = DeduplicateKafkaSinkTransformer.getMappingFromRetainedGlobalConfigToLocalConfig(config)

    // then
    mapping should contain theSameElementsAs Map(
      "reader.option.kafka.option1" -> "reader.option.kafka.option1",
      "reader.option.kafka.option2" -> "reader.option.kafka.option2",
      "writer.kafka.option.option3" -> "writer.kafka.option.option3",
      s"transformer.decoder.${ConfluentAvroDecodingTransformer.KEY_SCHEMA_REGISTRY_URL}" -> "deduplicateKafkaSinkTransformer.decoder.schema.registry.url",
      s"transformer.decoder.schema.registry.options.some.key" -> "deduplicateKafkaSinkTransformer.decoder.schema.registry.options.some.key",
      s"transformer.encoder.${ConfluentAvroEncodingTransformer.KEY_SCHEMA_REGISTRY_URL}" -> "deduplicateKafkaSinkTransformer.encoder.schema.registry.url",
      KafkaStreamReader.KEY_TOPIC -> KafkaStreamReader.KEY_TOPIC,
      KafkaStreamReader.KEY_BROKERS -> KafkaStreamReader.KEY_BROKERS,
      KafkaStreamWriter.KEY_TOPIC -> KafkaStreamWriter.KEY_TOPIC,
      KafkaStreamWriter.KEY_BROKERS -> KafkaStreamWriter.KEY_BROKERS,
      StreamWriterCommonAttributes.keyCheckpointBaseLocation -> StreamWriterCommonAttributes.keyCheckpointBaseLocation
    )
  }

  private def getLocalConfig() = {
    val config = getEmptyConfiguration
    config.addProperty(KafkaStreamReader.KEY_TOPIC, "readerTopic")
    config.addProperty(KafkaStreamReader.KEY_BROKERS, "http://readerBrokers:9092")
    config.addProperty("reader.option.kafka.security.protocol", "SASL_PLAINTEXT")
    config.addProperty("reader.option.failOnDataLoss", false)
    config.addProperty(s"$decoderPrefix.irrelevant.key", "irrelevant value")
    config.addProperty(s"$decoderPrefix.schema.registry.url", dummySourceRegistry)
    config.addProperty(s"$decoderPrefix.schema.registry.options.some.extra.config", "someDecoderExtraConfig")

    config.addProperty(KafkaStreamWriter.KEY_TOPIC, "writerTopic")
    config.addProperty(KafkaStreamWriter.KEY_BROKERS, "http://writerBrokers:9092")
    config.addProperty("writer.kafka.option.kafka.sasl.mechanism", "GSSAPI")
    config.addProperty("component.transformer.class.encoder", classOf[ConfluentAvroEncodingTransformer].getCanonicalName)
    config.addProperty(s"$encoderPrefix.schema.registry.url", dummyDestinationRegistry)
    config.addProperty(s"$encoderPrefix.schema.registry.options.some.extra.config", "someEncoderExtraConfig")

    config.addProperty(StreamWriterCommonAttributes.keyCheckpointBaseLocation, "/tmp/checkpoint")

    config.addProperty(DeduplicateKafkaSinkTransformer.SourceIdColumns, "offset, partition")
    config.addProperty(DeduplicateKafkaSinkTransformer.DestinationIdColumns, "value.hyperdrive_id.source_offset, value.hyperdrive_id.source_partition")
    config.addProperty(DeduplicateKafkaSinkTransformer.KafkaConsumerTimeout, 5)

    config
  }

  private def getEmptyConfiguration = {
    val config = new DynamicCombinedConfiguration()
    config.setListDelimiterHandler(new DefaultListDelimiterHandler(','))
    config
  }

}
