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

package org.apache.spark.sql.kafka010

import org.apache.kafka.common.TopicPartition
import org.apache.spark.sql.execution.streaming.Offset
import za.co.absa.hyperdrive.compatibility.provider.CompatibleOffset

object KafkaSourceOffsetProxy {
  def getPartitionOffsets(offset: CompatibleOffset.Type): Map[TopicPartition, Long] = {
    // TODO: Remove casting as soon as spark kafka is migrated to org.apache.spark.sql.connector.read.streaming.Offset
    KafkaSourceOffset.getPartitionOffsets(offset.asInstanceOf[Offset])
  }

  def apply(offsetTuples: (String, Int, Long)*): KafkaSourceOffset = {
    KafkaSourceOffset.apply(offsetTuples:_*)
  }
}
