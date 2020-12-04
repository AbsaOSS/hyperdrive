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
import org.scalatest.{FlatSpec, Matchers}
import za.co.absa.hyperdrive.ingestor.api.writer.StreamWriterCommonAttributes
import za.co.absa.hyperdrive.ingestor.implementation.reader.kafka.KafkaStreamReader
import za.co.absa.hyperdrive.ingestor.implementation.transformer.avro.confluent.{ConfluentAvroDecodingTransformer, ConfluentAvroEncodingTransformer}
import za.co.absa.hyperdrive.ingestor.implementation.writer.kafka.KafkaStreamWriter

class TestDeduplicateKafkaSinkTransformerObject extends FlatSpec with Matchers {
  behavior of DeduplicateKafkaSinkTransformer.getClass.getSimpleName

  "apply" should "create a DeduplicateKafkaSinkTransformer" in {
    // given
    val config = getLocalConfig()

    // when
    val transformer = DeduplicateKafkaSinkTransformer(config).asInstanceOf[DeduplicateKafkaSinkTransformer]

    // then
    transformer.readerTopic shouldBe "readerTopic"
    transformer.readerBrokers shouldBe "http://readerBrokers:9092"
    transformer.readerExtraOptions should contain theSameElementsAs Map(
      "kafka.security.protocol" -> "SASL_PLAINTEXT",
      "failOnDataLoss" -> "false"
    )
    transformer.readerSchemaRegistryUrl shouldBe "http://sourceRegistry:8081"
    transformer.writerTopic shouldBe "writerTopic"
    transformer.writerBrokers shouldBe "http://writerBrokers:9092"
    transformer.writerExtraOptions shouldBe Map(
      "kafka.sasl.mechanism" -> "GSSAPI"
    )
    transformer.writerSchemaRegistryUrl shouldBe "http://writerRegistry:8081"

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

  "getMappingFromRetainedGlobalConfigToLocalConfig" should "return the local config mapping" in {
    // given
    val config = getEmptyConfiguration
    config.addProperty("reader.option.kafka.option1", "value1")
    config.addProperty("reader.option.kafka.option2", "value2")
    config.addProperty("component.transformer.id.0", "decoder")
    config.addProperty("component.transformer.class.decoder", classOf[ConfluentAvroDecodingTransformer].getCanonicalName)
    config.addProperty(s"transformer.decoder.${ConfluentAvroDecodingTransformer.KEY_SCHEMA_REGISTRY_URL}", "http://sourceRegistry:8081")

    config.addProperty("component.transformer.id.1", "encoder")
    config.addProperty("component.transformer.class.encoder", classOf[ConfluentAvroEncodingTransformer].getCanonicalName)
    config.addProperty(s"transformer.encoder.${ConfluentAvroEncodingTransformer.KEY_SCHEMA_REGISTRY_URL}", "http://writerRegistry:8081")

    config.addProperty("writer.kafka.option.option3", "value3")
    // when
    val mapping = DeduplicateKafkaSinkTransformer.getMappingFromRetainedGlobalConfigToLocalConfig(config)

    // then
    mapping should contain theSameElementsAs Map(
      "reader.option.kafka.option1" -> "reader.option.kafka.option1",
      "reader.option.kafka.option2" -> "reader.option.kafka.option2",
      "writer.kafka.option.option3" -> "writer.kafka.option.option3",
      s"transformer.decoder.${ConfluentAvroDecodingTransformer.KEY_SCHEMA_REGISTRY_URL}" -> "deduplicateKafkaSinkTransformer.readerSchemaRegistryUrl",
      s"transformer.encoder.${ConfluentAvroEncodingTransformer.KEY_SCHEMA_REGISTRY_URL}" -> "deduplicateKafkaSinkTransformer.writerSchemaRegistryUrl",
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
    config.addProperty("deduplicateKafkaSinkTransformer.readerSchemaRegistryUrl", "http://sourceRegistry:8081")

    config.addProperty(KafkaStreamWriter.KEY_TOPIC, "writerTopic")
    config.addProperty(KafkaStreamWriter.KEY_BROKERS, "http://writerBrokers:9092")
    config.addProperty("writer.kafka.option.kafka.sasl.mechanism", "GSSAPI")
    config.addProperty("component.transformer.class.encoder", classOf[ConfluentAvroEncodingTransformer].getCanonicalName)
    config.addProperty("deduplicateKafkaSinkTransformer.writerSchemaRegistryUrl", "http://writerRegistry:8081")

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
