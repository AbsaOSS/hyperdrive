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
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import org.apache.spark.sql.execution.streaming.{CommitLog, OffsetSeqLog}
import org.apache.spark.sql.kafka010.KafkaSourceOffsetProxy
import za.co.absa.hyperdrive.compatibility.provider.CompatibleOffsetProvider
import za.co.absa.hyperdrive.ingestor.implementation.transformer.deduplicate.kafka.PrunedConsumerRecord

import scala.collection.JavaConverters._
import scala.collection.mutable

private[hyperdrive] object KafkaUtil {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def getAtLeastNLatestRecordsFromPartition[K, V](consumer: KafkaConsumer[K, V], numberOfRecords: Map[TopicPartition, Long],
                                                  pruningFn: ConsumerRecord[K, V] => PrunedConsumerRecord)
    (implicit kafkaConsumerTimeout: Duration): Seq[PrunedConsumerRecord] = {
    consumer.assign(numberOfRecords.keySet.asJava)
    val endOffsets = consumer.endOffsets(numberOfRecords.keySet.asJava).asScala.mapValues(Long2long)
    val topicPartitions = endOffsets.keySet

    var records: Seq[PrunedConsumerRecord] = Seq()
    val offsetLowerBounds = mutable.Map(endOffsets.toSeq: _*)
    import scala.util.control.Breaks._
    breakable {
      while (true) {
        val recordSizes = records
          .groupBy(r => new TopicPartition(r.topic, r.partition))
          .mapValues(records => records.size)
        val beginningOffsets = consumer.beginningOffsets(topicPartitions.asJava).asScala
        val unfinishedPartitions = topicPartitions.filter(p =>
          recordSizes.getOrElse(p, 0) < numberOfRecords(p) && offsetLowerBounds(p) > beginningOffsets(p))
        if (unfinishedPartitions.isEmpty) {
          break()
        }

        unfinishedPartitions.foreach { p =>
          offsetLowerBounds(p) = Math.max(0, offsetLowerBounds(p) - numberOfRecords(p))
        }
        offsetLowerBounds.foreach {
          case (partition, offset) => consumer.seek(partition, offset)
        }
        records = getMessagesAtLeastToOffset(consumer, endOffsets.toMap, pruningFn)
      }
    }

    records
  }

  def getMessagesAtLeastToOffset[K, V](consumer: KafkaConsumer[K, V], toOffsets: Map[TopicPartition, Long],
                                       pruningFn: ConsumerRecord[K, V] => PrunedConsumerRecord)
                                      (implicit kafkaConsumerTimeout: Duration): Seq[PrunedConsumerRecord] = {
    consumer.assign(toOffsets.keySet.asJava)
    val endOffsets = consumer.endOffsets(toOffsets.keys.toSeq.asJava).asScala
    endOffsets.foreach { case (topicPartition, offset) =>
      val toOffset = toOffsets(topicPartition)
      if (toOffset > offset) {
        throw new IllegalArgumentException(s"Requested consumption to offsets $toOffsets, but they cannot be higher " +
          s"than the end offsets, which are $endOffsets")
      }
    }

    import scala.util.control.Breaks._
    var records: Seq[PrunedConsumerRecord] = mutable.Seq()
    breakable {
      while (true) {
        val newRecords = consumer.poll(kafkaConsumerTimeout).asScala.toSeq
        records ++= newRecords.map(pruningFn)
        if (newRecords.isEmpty || offsetsHaveBeenReached(consumer, toOffsets)) {
          break()
        }
      }
    }

    toOffsets.foreach { case (tp, toOffset) =>
      val offsetAfterPoll = consumer.position(tp)
      if (offsetAfterPoll < toOffset) {
        throw new IllegalStateException(s"Expected to reach offset $toOffset on $tp, but only reached $offsetAfterPoll." +
          s" Not all expected messages were consumed. Consider increasing the consumer timeout")
      }
    }

    records
  }

  private def offsetsHaveBeenReached[K, V](consumer: KafkaConsumer[K, V], toOffsets: Map[TopicPartition, Long]) = {
    toOffsets.forall { case (tp, toOffset) =>
      val position = consumer.position(tp)
      position >= toOffset
    }
  }

  def seekToOffsetsOrBeginning[K, V](consumer: KafkaConsumer[K, V], topic: String, offsetsOpt: Option[Map[TopicPartition, Long]]): Unit = {
    val partitions = getTopicPartitions(consumer, topic)
    consumer.assign(partitions.asJava)
    offsetsOpt match {
      case Some(topicPartitionOffsets) => topicPartitionOffsets.foreach {
        case (topicPartition, offset) => consumer.seek(topicPartition, offset)
      }
      case None =>
        consumer.seekToBeginning(partitions.asJava)
    }
  }

  def getTopicPartitions[K, V](consumer: KafkaConsumer[K, V], topic: String): Seq[TopicPartition] = {
    consumer.partitionsFor(topic).asScala.map(p => new TopicPartition(p.topic(), p.partition()))
  }

  def getLatestOffset(offsetLog: OffsetSeqLog): Option[Map[TopicPartition, Long]] = {
    val offsetSeqOpt = offsetLog.getLatest().map(_._2.offsets)
    offsetSeqOpt.flatMap(parseOffsetSeq)
  }

  def getLatestCommittedOffset(offsetLog: OffsetSeqLog, commitLog: CommitLog): Option[Map[TopicPartition, Long]] = {
    val offsetSeqOpt = commitLog.getLatest().map(_._1)
      .map(batchId => offsetLog.get(batchId)
        .getOrElse(throw new IllegalStateException(s"No offset found for committed batchId ${batchId}")))
      .map(offsetLog => offsetLog.offsets)
    offsetSeqOpt.flatMap(parseOffsetSeq)
  }

  private def parseOffsetSeq(offsetSeq: Seq[Option[CompatibleOffsetProvider.Type]]) = {
    if (offsetSeq.size == 1) {
      if (offsetSeq.head.isDefined) {
        Some(KafkaSourceOffsetProxy.getPartitionOffsets(offsetSeq.head.get))
      } else {
        None
      }
    } else {
      throw new IllegalStateException(s"Cannot support more than 1 source, got ${offsetSeq.toString}")
    }
  }
}
