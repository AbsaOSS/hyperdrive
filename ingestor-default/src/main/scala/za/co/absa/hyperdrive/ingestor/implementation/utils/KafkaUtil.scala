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
import java.{lang, util}
import java.util.Collections

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.sql.execution.streaming.{CommitLog, OffsetSeqLog}
import za.co.absa.hyperdrive.ingestor.implementation.transformer.deduplicate.kafka.kafka010.KafkaSourceOffset

import scala.collection.mutable
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
      records = getAllAvailableMessages(consumer)
    }

    records
  }

//  TODO: Add from, to arguments. Caller should know what beginning and end offsets to expect
  def getAllAvailableMessages[K, V](consumer: KafkaConsumer[K, V])(implicit kafkaConsumerTimeout: Duration): Seq[ConsumerRecord[K, V]] = {
    import scala.util.control.Breaks._
    var records: Seq[ConsumerRecord[K, V]] = mutable.Seq()
    breakable {
      while (true) {
        val newRecords = consumer.poll(kafkaConsumerTimeout).asScala.toSeq
        if (newRecords.isEmpty) {
          break()
        }
        records ++= newRecords
      }
    }
    records
  }

  /**
   * Determines the latest committed offsets by inspecting structured streaming's offset log and commit log.
   * If no committed offsets are available, seeks to beginning.
   */
  def seekToLatestCommittedOffsets[K, V](consumer: KafkaConsumer[K, V], topic: String, offsetLog: OffsetSeqLog, commitLog: CommitLog): Unit = {
    val sourceTopicPartitionOffsetsOpt = getTopicPartitionsFromLatestCommittedOffsets(offsetLog, commitLog)
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

  private def getTopicPartitionsFromLatestCommittedOffsets(offsetLog: OffsetSeqLog, commitLog: CommitLog): Option[Map[TopicPartition, Long]] = {
    val offsetSeqOpt = commitLog.getLatest().map(_._1)
      .flatMap(batchId => offsetLog.get(batchId))
      .map(offsetLog => offsetLog.offsets)
    val result = if (offsetSeqOpt.isDefined) {
      if (offsetSeqOpt.get.size == 1) {
        if (offsetSeqOpt.get.head.isDefined) {
          Some(KafkaSourceOffset.getPartitionOffsets(offsetSeqOpt.get.head.get))
        } else {
          throw new IllegalStateException("Offset must be defined, got None")
        }
      } else {
        throw new IllegalStateException(s"Cannot support more than 1 source, got ${offsetSeqOpt.toString}")
      }
    } else {
      None
    }
    result
  }
}
