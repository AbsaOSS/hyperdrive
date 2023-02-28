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

package za.co.absa.hyperdrive.ingestor.implementation.writer.kafka

import java.util.concurrent.TimeUnit
import org.apache.commons.configuration2.BaseConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.mockito.AdditionalAnswers._
import org.mockito.ArgumentMatchers._
import org.mockito.ArgumentMatchers.{eq => eqTo}
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.prop.TableDrivenPropertyChecks
import za.co.absa.hyperdrive.ingestor.api.writer.StreamWriterCommonAttributes.{keyCheckpointBaseLocation, keyTriggerProcessingTime, keyTriggerType}
import za.co.absa.hyperdrive.ingestor.api.writer.StreamWriterProperties
import za.co.absa.hyperdrive.ingestor.implementation.writer.kafka.KafkaStreamWriter._

class TestKafkaStreamWriter extends AnyFlatSpec with Matchers with MockitoSugar with TableDrivenPropertyChecks {

  behavior of KafkaStreamWriter.getClass.getSimpleName

  it should "create a KafkaStreamWriter" in {
    val config = new BaseConfiguration()
    config.addProperty(KEY_TOPIC, "thetopic")
    config.addProperty(KEY_BROKERS, "brokers")
    config.addProperty("writer.kafka.option.extra-key", "ExtraValue")
    config.addProperty("writer.kafka.option.extra-key-2", "ExtraValue2")
    config.addProperty(keyTriggerType, "ProcessingTime")
    config.addProperty(keyTriggerProcessingTime, "10000")
    config.addProperty(keyCheckpointBaseLocation, "/tmp/checkpoint-location")

    val dataStreamWriterMock = getDataStreamWriterMock()
    val dataFrameMock = getDataFrameMock(dataStreamWriterMock)

    val writer = KafkaStreamWriter(config).asInstanceOf[KafkaStreamWriter]
    writer.write(dataFrameMock)

    verify(dataStreamWriterMock).options(eqTo(Map("extra-key" -> "ExtraValue", "extra-key-2" -> "ExtraValue2")))
    verify(dataStreamWriterMock).option("topic", "thetopic")
    verify(dataStreamWriterMock).option("kafka.bootstrap.servers", "brokers")
    verify(dataStreamWriterMock).option(StreamWriterProperties.CheckpointLocation, "/tmp/checkpoint-location")
    verify(dataStreamWriterMock).format("kafka")
    verify(dataStreamWriterMock).trigger(eqTo(Trigger.ProcessingTime(10000L, TimeUnit.MILLISECONDS)))
  }

  private def getDataStreamWriterMock() = {
    val dataStreamWriterMock = mock[DataStreamWriter[Row]](withSettings().defaultAnswer(RETURNS_SELF))
    val streamingQueryMock = mock[StreamingQuery]
    when(dataStreamWriterMock.start()).thenReturn(streamingQueryMock)
    dataStreamWriterMock
  }

  private def getDataFrameMock(dataStreamWriter: DataStreamWriter[Row]) = {
    val dataFrameMock = mock[DataFrame](withSettings().defaultAnswer(RETURNS_SELF))
    when(dataFrameMock.writeStream).thenReturn(dataStreamWriter)
    when(dataFrameMock.columns).thenReturn(Array("col1", "col2"))

    val sparkSessionMock = mock[SparkSession]
    val sparkContextMock = mock[SparkContext]
    val hadoopConfigurationMock = mock[Configuration]
    when(dataFrameMock.sparkSession).thenReturn(sparkSessionMock)
    when(sparkSessionMock.sparkContext).thenReturn(sparkContextMock)
    when(sparkContextMock.hadoopConfiguration).thenReturn(hadoopConfigurationMock)
    dataFrameMock
  }
}
