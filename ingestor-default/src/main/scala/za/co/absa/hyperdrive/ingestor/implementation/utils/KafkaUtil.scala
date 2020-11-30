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

package za.co.absa.hyperdrive.ingestor.implementation.utils

import java.time.Duration
import java.util
import java.util.Collections

import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.sql.execution.streaming.{CommitLog, OffsetSeqLog}
import za.co.absa.hyperdrive.ingestor.implementation.transformer.deduplicate.kafka.kafka010.KafkaSourceOffset

import scala.collection.JavaConverters._

object KafkaUtil {

  def getAtLeastNLatestRecords[K, V](consumer: KafkaConsumer[K, V], topicPartition: TopicPartition, numberOfRecords: Int)(implicit kafkaConsumerTimeout: Duration): Seq[ConsumerRecord[K, V]] = {
    consumer.assign(Seq(topicPartition).asJava)
    val endOffsets = consumer.endOffsets(Seq(topicPartition).asJava).asScala
    if (endOffsets.size != 1) {
      throw new IllegalStateException(s"Expected exactly 1 end offset, got ${endOffsets}")
    }
    val partition = endOffsets.keys.head
    val offset = endOffsets.values.head

    var records: Seq[ConsumerRecord[K, V]] = Seq()
    var offsetLowerBound = offset
    while (records.size < numberOfRecords && offsetLowerBound != 0) {
      offsetLowerBound = Math.max(0, offsetLowerBound - numberOfRecords)
      consumer.seek(partition, offsetLowerBound)
      records = getMessagesAtLeastToOffset(consumer, Map(topicPartition -> offset))
    }

    records
  }

  def getMessagesAtLeastToOffset[K, V](consumer: KafkaConsumer[K, V], toOffsets: Map[TopicPartition, Long])
    (implicit kafkaConsumerTimeout: Duration): Seq[ConsumerRecord[K, V]] = {
    val endOffsets = consumer.endOffsets(toOffsets.keys.toSeq.asJava).asScala
    endOffsets.foreach { case (topicPartition, offset) =>
      val toOffset = toOffsets(topicPartition)
      if (toOffset > offset) {
        throw new IllegalArgumentException(s"Requested consumption to offsets $toOffsets, but they cannot be higher " +
          s"than the end offsets, which are $endOffsets")
      }
    }

    val records = consumer.poll(kafkaConsumerTimeout).asScala.toSeq

    toOffsets.foreach { case (tp, toOffset) =>
      if (!consumer.assignment().contains(tp)) {
        throw new IllegalStateException(s"Consumer is unexpectedly not assigned to $tp. Consider increasing the consumer timeout")
      }
      val offsetAfterPoll = consumer.position(tp)
      if (offsetAfterPoll < toOffset) {
        throw new IllegalStateException(s"Expected to reach offset $toOffset on $tp, but only reached $offsetAfterPoll." +
          s" Not all expected messages were consumed. Consider increasing the consumer timeout")
      }
    }

    records
  }

  /**
   * Determines the latest committed offsets by inspecting structured streaming's offset log and commit log.
   * If no committed offsets are available, seeks to beginning.
   */
  def seekToLatestCommittedOffsets[K, V](consumer: KafkaConsumer[K, V], topic: String, offsetLog: OffsetSeqLog, commitLog: CommitLog): Unit = {
    val sourceTopicPartitionOffsetsOpt = getTopicPartitionsFromLatestCommit(offsetLog, commitLog)
    consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener {
      override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {}

      override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
        sourceTopicPartitionOffsetsOpt match {
          case Some(topicPartitionOffsets) => topicPartitionOffsets.foreach {
            case (topicPartition, offset) => consumer.seek(topicPartition, offset)
          }
          case None =>
            val partitions = getTopicPartitions(consumer, topic)
            consumer.seekToBeginning(partitions.asJava)
        }
      }
    })
  }

  def getTopicPartitions[K, V](consumer: KafkaConsumer[K, V], topic: String): Seq[TopicPartition] = {
    consumer.partitionsFor(topic).asScala.map(p => new TopicPartition(p.topic(), p.partition()))
  }

  private def getTopicPartitionsFromLatestCommit(offsetLog: OffsetSeqLog, commitLog: CommitLog): Option[Map[TopicPartition, Long]] = {
    val offsetSeqOpt = commitLog.getLatest().map(_._1)
      .flatMap(batchId => offsetLog.get(batchId))
      .map(offsetLog => offsetLog.offsets)
    offsetSeqOpt.map(offsetSeq =>
      if (offsetSeq.size == 1) {
        if (offsetSeq.head.isDefined) {
          KafkaSourceOffset.getPartitionOffsets(offsetSeq.head.get)
        } else {
          throw new IllegalStateException("Offset must be defined, got None")
        }
      } else {
        throw new IllegalStateException(s"Cannot support more than 1 source, got ${offsetSeqOpt.toString}")
      }
    )
  }

  def getTopicPartitionsFromLatestOffset(offsetLog: OffsetSeqLog): Option[Map[TopicPartition, Long]] = {
    val offsetSeqOpt = offsetLog.getLatest().map(_._2.offsets)
    offsetSeqOpt.map(offsetSeq =>
      if (offsetSeq.size == 1) {
        if (offsetSeq.head.isDefined) {
          KafkaSourceOffset.getPartitionOffsets(offsetSeq.head.get)
        } else {
          throw new IllegalStateException("Offset must be defined, got None")
        }
      } else {
        throw new IllegalStateException(s"Cannot support more than 1 source, got ${offsetSeqOpt.toString}")
      }
    )
  }
}
