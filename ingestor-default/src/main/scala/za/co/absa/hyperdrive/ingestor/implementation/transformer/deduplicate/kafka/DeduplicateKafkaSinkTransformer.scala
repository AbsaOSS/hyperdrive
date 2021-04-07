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
import java.util.{Properties, UUID}
import za.co.absa.hyperdrive.ingestor.api.utils.ConfigUtils
import za.co.absa.hyperdrive.ingestor.implementation.transformer.avro.confluent.{ConfluentAvroDecodingTransformer, ConfluentAvroEncodingTransformer}
import za.co.absa.hyperdrive.ingestor.implementation.utils.KafkaUtil
import org.apache.avro.generic.GenericRecord
import org.apache.commons.configuration2.Configuration
import org.apache.hadoop.fs.Path
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming.{CommitLog, OffsetSeqLog}
import org.apache.spark.sql.functions.{col, lit, not, struct}
import za.co.absa.hyperdrive.ingestor.api.transformer.{StreamTransformer, StreamTransformerFactory}
import za.co.absa.hyperdrive.ingestor.api.utils.ConfigUtils.{getOrThrow, getPropertySubset, getSeqOrThrow}
import za.co.absa.hyperdrive.ingestor.api.utils.StreamWriterUtil
import za.co.absa.hyperdrive.ingestor.api.writer.StreamWriterCommonAttributes
import za.co.absa.hyperdrive.ingestor.implementation.reader.kafka.KafkaStreamReader
import za.co.absa.hyperdrive.ingestor.implementation.utils.AvroUtil
import za.co.absa.hyperdrive.ingestor.implementation.writer.kafka.KafkaStreamWriter


private[transformer] class DeduplicateKafkaSinkTransformer(
  val readerTopic: String,
  val readerBrokers: String,
  val readerExtraOptions: Map[String, String],
  val readerSchemaRegistryUrl: String,
  val writerTopic: String,
  val writerBrokers: String,
  val writerExtraOptions: Map[String, String],
  val writerSchemaRegistryUrl: String,
  val checkpointLocation: String,
  val sourceIdColumnNames: Seq[String],
  val destinationIdColumnNames: Seq[String],
  val kafkaConsumerTimeout: Duration
) extends StreamTransformer {
  private val logger = LogManager.getLogger

  override def transform(dataFrame: DataFrame): DataFrame = {
    val spark = dataFrame.sparkSession
    val offsetLog = new OffsetSeqLog(spark, new Path(checkpointLocation, "offsets").toString)
    val commitLog = new CommitLog(spark, new Path(checkpointLocation, "commits").toString)
    val latestOffsetLog = offsetLog.getLatest().map(_._1)
    val latestCommitLog = commitLog.getLatest().map(_._1)

    if (latestOffsetLog != latestCommitLog) {
      deduplicateDataFrame(dataFrame, offsetLog, commitLog)
    } else {
      dataFrame
    }
  }

  private def deduplicateDataFrame(dataFrame: DataFrame, offsetLog: OffsetSeqLog, commitLog: CommitLog) = {
    logger.info("Deduplicate rows after retry")
    implicit val kafkaConsumerTimeoutImpl: Duration = kafkaConsumerTimeout
    val sourceConsumer = createConsumer(readerBrokers, readerExtraOptions, readerSchemaRegistryUrl)
    val latestCommittedOffsets = KafkaUtil.getLatestCommittedOffset(offsetLog, commitLog)
    logCommittedOffsets(latestCommittedOffsets)

    KafkaUtil.seekToOffsetsOrBeginning(sourceConsumer, readerTopic, latestCommittedOffsets)
    logCurrentPositions(sourceConsumer)

    val latestOffsetsOpt = KafkaUtil.getLatestOffset(offsetLog)
    logOffsets(latestOffsetsOpt)

    val sourceRecords = latestOffsetsOpt.map(latestOffset => consumeAndClose(sourceConsumer,
      consumer => KafkaUtil.getMessagesAtLeastToOffset(consumer, latestOffset))).getOrElse(Seq())
    val sourceIds = sourceRecords.map(extractIdFieldsFromRecord(_, sourceIdColumnNames))

    val sinkConsumer = createConsumer(writerBrokers, writerExtraOptions, writerSchemaRegistryUrl)
    val sinkTopicPartitions = KafkaUtil.getTopicPartitions(sinkConsumer, writerTopic)
    val recordsPerPartition = sinkTopicPartitions.map(p => p -> sourceRecords.size.toLong).toMap
    val latestSinkRecords = consumeAndClose(sinkConsumer, consumer =>
      KafkaUtil.getAtLeastNLatestRecordsFromPartition(consumer, recordsPerPartition))
    logConsumedSinkRecords(latestSinkRecords)

    val publishedIds = latestSinkRecords.map(extractIdFieldsFromRecord(_, destinationIdColumnNames))
    val duplicatedIds = sourceIds.intersect(publishedIds)
    logDuplicatedIds(duplicatedIds)
    val duplicatedIdsLit = duplicatedIds.map(duplicatedId => struct(duplicatedId.map(lit): _*))
    val idColumns = sourceIdColumnNames.map(col)
    dataFrame.filter(not(struct(idColumns: _*).isInCollection(duplicatedIdsLit)))
  }

  private def logCommittedOffsets(offsets: Option[Map[TopicPartition, Long]]): Unit =
    logger.info(s"Latest committed source offsets by partition for ${readerTopic}: { ${offsetsToString(offsets)} }" )

  private def logOffsets(offsets: Option[Map[TopicPartition, Long]]): Unit =
    logger.info(s"Latest source offsets by partition for ${readerTopic}: { ${offsetsToString(offsets)} }" )

  private def offsetsToString(offsets: Option[Map[TopicPartition, Long]]) = {
    offsets.map(_.toSeq
      .sortBy{ case (tp, _) => tp.partition()}
      .map{ case (tp, offset) => s"${tp.partition()}: $offset"}.reduce(_ + ", " + _)).getOrElse("-")
  }

  private def logCurrentPositions(consumer: KafkaConsumer[GenericRecord, GenericRecord]): Unit = {
    val sourcePartitions = KafkaUtil.getTopicPartitions(consumer, readerTopic)
    val currentPositions = sourcePartitions
      .sortBy(_.partition())
      .map { tp => s"${tp.partition()}: ${consumer.position(tp)}"}.reduce(_ + ", " + _)
    logger.info(s"Reset source offsets by partition to { ${currentPositions} }")
  }

  private def logConsumedSinkRecords(latestSinkRecords: Seq[ConsumerRecord[GenericRecord, GenericRecord]]): Unit = {
    val offsetsByPartition = latestSinkRecords.map(r => r.partition() -> r.offset())
      .groupBy(_._1)
      .mapValues(_.map(_._2))
      .toSeq
      .sortBy(_._1)
    val firstOffsets = offsetsByPartition.map { case (partition, offsets) => s"$partition: ${offsets.take(3)}"}.reduce(_ + ", " + _)
    val lastOffsets = offsetsByPartition.map { case (partition, offsets) => s"$partition: ${offsets.takeRight(3)}"}.reduce(_ + ", " + _)
    logger.info(s"Consumed ${latestSinkRecords.size} sink records. First three offsets by partition: { ${firstOffsets} }. Last three offsets: { ${lastOffsets} }")
  }

  private def logDuplicatedIds(duplicatedIds: Seq[Seq[Any]]): Unit = {
    logger.info(s"Found ${duplicatedIds.size} duplicated ids. First three: ${duplicatedIds.take(3)}.")
  }

  private def extractIdFieldsFromRecord(record: ConsumerRecord[GenericRecord, GenericRecord], idColumnNames: Seq[String]): Seq[Any] = {
    idColumnNames.map(idColumnName =>
      AvroUtil.getFromConsumerRecord(record, idColumnName)
        .getOrElse(throw new IllegalArgumentException(s"Could not find value for field $idColumnName"))
    )
  }

  private def consumeAndClose[T](consumer: KafkaConsumer[GenericRecord, GenericRecord], consume: KafkaConsumer[GenericRecord, GenericRecord] => T) = {
    try {
      consume(consumer)
    } catch {
      case throwable: Throwable => logger.error(s"An unexpected error occurred while consuming", throwable)
        throw throwable
    } finally {
      consumer.close()
    }
  }

  private def createConsumer(brokers: String, extraOptions: Map[String, String], schemaRegistryUrl: String) = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, s"hyperdrive_consumer_${UUID.randomUUID().toString}")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, s"hyperdrive_group_${UUID.randomUUID().toString}")
    extraOptions.foreach {
      case (key, value) => props.put(key, value)
    }
    props.put("schema.registry.url", schemaRegistryUrl)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer")
    new KafkaConsumer[GenericRecord, GenericRecord](props)
  }
}

object DeduplicateKafkaSinkTransformer extends StreamTransformerFactory with DeduplicateKafkaSinkTransformerAttributes {

  private val DefaultKafkaConsumerTimeoutSeconds = 120L

  private val readerSchemaRegistryUrlKey = "deduplicateKafkaSinkTransformer.readerSchemaRegistryUrl"
  private val writerSchemaRegistryUrlKey = "deduplicateKafkaSinkTransformer.writerSchemaRegistryUrl"

  override def apply(config: Configuration): StreamTransformer = {
    val readerTopic = getOrThrow(KafkaStreamReader.KEY_TOPIC, config)
    val readerBrokers = getOrThrow(KafkaStreamReader.KEY_BROKERS, config)
    val readerExtraOptions = KafkaStreamReader.getExtraConfigurationPrefix.map(getPropertySubset(config, _)).getOrElse(Map())
    val readerSchemaRegistryUrl = getOrThrow(readerSchemaRegistryUrlKey, config)

    val writerTopic = getOrThrow(KafkaStreamWriter.KEY_TOPIC, config)
    val writerBrokers = getOrThrow(KafkaStreamWriter.KEY_BROKERS, config)
    val writerExtraOptions = KafkaStreamWriter.getExtraConfigurationPrefix.map(getPropertySubset(config, _)).getOrElse(Map())
    val writerSchemaRegistryUrl = getOrThrow(writerSchemaRegistryUrlKey, config)

    val checkpointLocation = StreamWriterUtil.getCheckpointLocation(config)

    val sourceIdColumns = getSeqOrThrow(SourceIdColumns, config)
    val destinationIdColumns = getSeqOrThrow(DestinationIdColumns, config)
    if (sourceIdColumns.size != destinationIdColumns.size) {
      throw new IllegalArgumentException("The size of source id column names doesn't match the list of destination id column names " +
        s"${sourceIdColumns.size} != ${destinationIdColumns.size}.")
    }

    val kafkaConsumerTimeout = Duration.ofSeconds(config.getLong(KafkaConsumerTimeout, DefaultKafkaConsumerTimeoutSeconds))

    new DeduplicateKafkaSinkTransformer(readerTopic, readerBrokers, readerExtraOptions, readerSchemaRegistryUrl,
      writerTopic, writerBrokers, writerExtraOptions, writerSchemaRegistryUrl,
      checkpointLocation, sourceIdColumns, destinationIdColumns, kafkaConsumerTimeout)
  }

  override def getMappingFromRetainedGlobalConfigToLocalConfig(globalConfig: Configuration): Map[String, String] = {
    import scala.collection.JavaConverters._
    val readerExtraOptionsKeys =
      KafkaStreamReader.getExtraConfigurationPrefix.map(globalConfig.getKeys(_).asScala.toSeq).getOrElse(Seq())
    val writerExtraOptionsKeys =
      KafkaStreamWriter.getExtraConfigurationPrefix.map(globalConfig.getKeys(_).asScala.toSeq).getOrElse(Seq())
    val keys = readerExtraOptionsKeys ++ writerExtraOptionsKeys ++
      Seq(
        KafkaStreamReader.KEY_TOPIC,
        KafkaStreamReader.KEY_BROKERS,
        KafkaStreamWriter.KEY_TOPIC,
        KafkaStreamWriter.KEY_BROKERS,
        StreamWriterCommonAttributes.keyCheckpointBaseLocation
      )
    val oneToOneMappings = keys.map(e => e -> e).toMap

    val readerSchemaRegistryUrlGlobalKey = getSchemaRegistryUrlKey(globalConfig, classOf[ConfluentAvroDecodingTransformer],
      ConfluentAvroDecodingTransformer.KEY_SCHEMA_REGISTRY_URL)
    val writerSchemaRegistryUrlGlobalKey = getSchemaRegistryUrlKey(globalConfig, classOf[ConfluentAvroEncodingTransformer],
      ConfluentAvroEncodingTransformer.KEY_SCHEMA_REGISTRY_URL)

    oneToOneMappings ++ Map(
      readerSchemaRegistryUrlGlobalKey -> readerSchemaRegistryUrlKey,
      writerSchemaRegistryUrlGlobalKey -> writerSchemaRegistryUrlKey
    )
  }

  private def getSchemaRegistryUrlKey[T <: StreamTransformer](config: Configuration, transformerClass: Class[T], transformerKey: String) = {
    val prefix = ConfigUtils.getTransformerPrefix(config, transformerClass).getOrElse(throw new IllegalArgumentException(
      s"Could not find transformer configuration for ${transformerClass.getCanonicalName}, but it is required"))

    s"${StreamTransformerFactory.TransformerKeyPrefix}.${prefix}.${transformerKey}"
  }

}


