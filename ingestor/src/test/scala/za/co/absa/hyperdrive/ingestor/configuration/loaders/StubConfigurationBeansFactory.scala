/*
 *  Copyright 2019 ABSA Group Limited
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package za.co.absa.hyperdrive.ingestor.configuration.loaders

import java.util.UUID

import za.co.absa.abris.avro.schemas.policy.SchemaRetentionPolicies
import za.co.absa.hyperdrive.ingestor.configuration.components._

object StubConfigurationBeansFactory {

  private var payloadTestTopic = randomString

  def resetTestTopic(): Unit = payloadTestTopic = randomString

  def getSparkSessionConf: SparkConf = {
    SparkConf(appName = "test_app-name")
  }

  def getStreamReaderConf: KafkaStreamReaderConf = {
   KafkaStreamReaderConf(topic = payloadTestTopic,
      brokers = "PLAINTEXT://broker1:9091,SSL://broker2.9092",
      Map("key1" -> "value1", "key2" -> "value2"))
  }

  def getOffsetManagerConf: CheckpointingOffsetManagerConf = {
    CheckpointingOffsetManagerConf(topic = payloadTestTopic, checkpointBaseLocation = "/tmp/test-checkpoint")
  }

  def getStreamDecoderConf: AvroStreamDecoderConf = {
    AvroStreamDecoderConf(randomString, Map(randomString -> randomString, randomString -> randomString),
      SchemaRetentionPolicies.RETAIN_SELECTED_COLUMN_ONLY)
  }

  def getStreamWriterConf: ParquetStreamWriterConf = {
    ParquetStreamWriterConf(randomString, Some(Map[String,String](randomString -> randomString)))
  }

  def getStreamWriterWithoutExtraConf: ParquetStreamWriterConf = {
    ParquetStreamWriterConf(randomString, None)
  }

  def getStreamTransformerConf: ColumnSelectorStreamTransformerConf = {
    ColumnSelectorStreamTransformerConf(columns = Seq("a","b","c"))
  }

  def randomString: String = UUID.randomUUID().toString
}
