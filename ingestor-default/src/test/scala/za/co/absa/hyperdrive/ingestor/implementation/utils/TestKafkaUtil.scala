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

import org.apache.kafka.common.TopicPartition
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.kafka010.KafkaSourceOffsetProxy
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import za.co.absa.commons.io.TempDirectory
import za.co.absa.commons.spark.SparkTestBase

class TestKafkaUtil extends FlatSpec with Matchers with BeforeAndAfter with SparkTestBase {
  private var baseDir: TempDirectory = _

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
}
