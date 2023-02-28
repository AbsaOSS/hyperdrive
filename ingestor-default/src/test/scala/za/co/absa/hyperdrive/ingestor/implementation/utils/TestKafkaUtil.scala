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

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.kafka010.KafkaSourceOffsetProxy
import org.mockito.ArgumentMatchers.{any, eq => eqTo}
import org.mockito.Mockito.{times, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatestplus.mockito.MockitoSugar.mock
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.commons.io.TempDirectory
import za.co.absa.hyperdrive.ingestor.implementation.transformer.deduplicate.kafka.PrunedConsumerRecord
import za.co.absa.spark.commons.test.SparkTestBase

import java.time.Duration
import java.util

class TestKafkaUtil extends AnyFlatSpec with Matchers with BeforeAndAfter with SparkTestBase {
  private var baseDir: TempDirectory = _
  private val kafkaSufficientTimeout = Duration.ofSeconds(5L)

  before {
    baseDir = TempDirectory("test-dir").deleteOnExit()
  }

  after{
    baseDir.delete()
  }

  "getLatestOffset" should "return the latest offsets" in {
    // given
    val offset0 = OffsetSeq.fill(KafkaSourceOffsetProxy(("t", 0, 10L), ("t", 1, 110L)))
    val offset1 = OffsetSeq.fill(KafkaSourceOffsetProxy(("t", 0, 25L), ("t", 1, 125L)))
    val offset2 = OffsetSeq.fill(KafkaSourceOffsetProxy(("t", 0, 42L), ("t", 1, 142L)))

    val offsetSeqLog = new OffsetSeqLog(spark, baseDir.path.toString)
    offsetSeqLog.add(0, offset0)
    offsetSeqLog.add(1, offset1)
    offsetSeqLog.add(2, offset2)

    // when
    val latestOffset = KafkaUtil.getLatestOffset(offsetSeqLog)

    // then
    latestOffset.get should contain theSameElementsAs Map(
      new TopicPartition("t", 0) -> 42L,
      new TopicPartition("t", 1) -> 142L
    )
  }

  it should "return None if there are no offsets" in {
    val offsetSeqLog = new OffsetSeqLog(spark, baseDir.path.toString)

    val latestOffset = KafkaUtil.getLatestOffset(offsetSeqLog)

    latestOffset shouldBe None
  }

  it should "return None if the offset is not defined" in {
    // given
    val offset = OffsetSeq.fill(null: Offset)
    val offsetSeqLog = new OffsetSeqLog(spark, baseDir.path.toString)
    offsetSeqLog.add(0, offset)

    // when
    val result = KafkaUtil.getLatestOffset(offsetSeqLog)

    // then
    result shouldBe None
  }

  it should "throw an exception if the offsets contain multiple sources" in {
    // given
    val offset = OffsetSeq.fill(KafkaSourceOffsetProxy(("t", 0, 10L)), KafkaSourceOffsetProxy(("t2", 1, 110L)))
    val offsetSeqLog = new OffsetSeqLog(spark, baseDir.path.toString)
    offsetSeqLog.add(0, offset)

    // when
    val exception = the[Exception] thrownBy KafkaUtil.getLatestOffset(offsetSeqLog)

    // then
    exception.getMessage should include("Cannot support more than 1 source")
  }

  "getLatestCommittedOffset" should "return the latest committed offset" in {
    // given
    val commitLog = new CommitLog(spark, s"${baseDir.path.toString}/commits")
    commitLog.add(0, CommitMetadata())
    commitLog.add(1, CommitMetadata())

    val offset0 = OffsetSeq.fill(KafkaSourceOffsetProxy(("t", 0, 10L), ("t", 1, 110L)))
    val offset1 = OffsetSeq.fill(KafkaSourceOffsetProxy(("t", 0, 25L), ("t", 1, 125L)))
    val offset2 = OffsetSeq.fill(KafkaSourceOffsetProxy(("t", 0, 42L), ("t", 1, 142L)))

    val offsetSeqLog = new OffsetSeqLog(spark, s"${baseDir.path.toString}/offsets")
    offsetSeqLog.add(0, offset0)
    offsetSeqLog.add(1, offset1)
    offsetSeqLog.add(2, offset2)

    // when
    val actualOffset = KafkaUtil.getLatestCommittedOffset(offsetSeqLog, commitLog)

    // then
    actualOffset.get should contain theSameElementsAs Map(
      new TopicPartition("t", 0) -> 25L,
      new TopicPartition("t", 1) -> 125L
    )
  }

  it should "return None if there is no commit" in {
    val commitLog = new CommitLog(spark, s"${baseDir.path.toString}/commits")

    val offset0 = OffsetSeq.fill(KafkaSourceOffsetProxy(("t", 0, 10L), ("t", 1, 110L)))
    val offset1 = OffsetSeq.fill(KafkaSourceOffsetProxy(("t", 0, 25L), ("t", 1, 125L)))
    val offset2 = OffsetSeq.fill(KafkaSourceOffsetProxy(("t", 0, 42L), ("t", 1, 142L)))

    val offsetSeqLog = new OffsetSeqLog(spark, s"${baseDir.path.toString}/offsets")
    offsetSeqLog.add(0, offset0)
    offsetSeqLog.add(1, offset1)
    offsetSeqLog.add(2, offset2)

    // when
    val actualOffset = KafkaUtil.getLatestCommittedOffset(offsetSeqLog, commitLog)

    // then
    actualOffset shouldBe None
  }

  it should "return throw an exception if there is no offset corresponding to the commit" in {
    // given
    val commitLog = new CommitLog(spark, s"${baseDir.path.toString}/commits")
    commitLog.add(0, CommitMetadata())
    commitLog.add(1, CommitMetadata())

    val offset0 = OffsetSeq.fill(KafkaSourceOffsetProxy(("t", 0, 10L), ("t", 1, 110L)))

    val offsetSeqLog = new OffsetSeqLog(spark, s"${baseDir.path.toString}/offsets")
    offsetSeqLog.add(0, offset0)

    // when
    val result = the[Exception] thrownBy KafkaUtil.getLatestCommittedOffset(offsetSeqLog, commitLog)

    // then
    result.getMessage should include ("batchId 1")
  }

  "getAtLeastNLatestRecordsFromPartition" should "poll only once if no more message is available" in {
    val topic = "topic"
    val topicPartition0 = new TopicPartition(topic, 0)
    val topicPartition1 = new TopicPartition(topic, 1)
    val startOffset0 = 280L
    val startOffset1 = 480L
    val endOffset0 = 300L
    val endOffset1 = 500L

    val pruningFn = (r: ConsumerRecord[String, String]) => PrunedConsumerRecord(
      r.topic(),
      r.partition(),
      r.offset(),
      Seq(r.value())
    )
    val mockKafkaConsumer = mock[KafkaConsumer[String, String]]
    val numberOfRecords = Map(topicPartition0 -> 50L, topicPartition1 -> 50L)
    import scala.collection.JavaConverters._
    val endOffsets = Map(topicPartition0 -> endOffset0, topicPartition1 -> endOffset1).asJava
    val beginningOffsets = Map(topicPartition0 -> startOffset0, topicPartition1 -> startOffset1).asJava
    val records0 = (startOffset0 to endOffset0)
      .map(i => new ConsumerRecord("topic", 0, i, "key", s"value_$i"))
      .asJava
    val records1 = (startOffset1 to endOffset1)
      .map(i => new ConsumerRecord("topic", 0, i, "key", s"value_$i"))
      .asJava
    val records = new ConsumerRecords(Map(topicPartition0 -> records0, topicPartition1 -> records1).asJava)

    when(mockKafkaConsumer.endOffsets(any())).thenAnswer(new Answer[java.util.Map[TopicPartition, Long]] {
      override def answer(invocation: InvocationOnMock): util.Map[TopicPartition, Long] = endOffsets
    })
    when(mockKafkaConsumer.beginningOffsets(any())).thenAnswer(new Answer[java.util.Map[TopicPartition, Long]] {
      override def answer(invocation: InvocationOnMock): util.Map[TopicPartition, Long] = beginningOffsets
    })
    when(mockKafkaConsumer.poll(any[Duration])).thenReturn(records)
    when(mockKafkaConsumer.position(eqTo(topicPartition0))).thenReturn(endOffset0)
    when(mockKafkaConsumer.position(eqTo(topicPartition1))).thenReturn(endOffset1)

    implicit val kafkaConsumerTimeout: Duration = kafkaSufficientTimeout
    val consumerRecords = KafkaUtil.getAtLeastNLatestRecordsFromPartition(mockKafkaConsumer, numberOfRecords, pruningFn)
    consumerRecords.size shouldBe records0.size + records1.size
    verify(mockKafkaConsumer, times(1)).poll(any[Duration])
  }
}
