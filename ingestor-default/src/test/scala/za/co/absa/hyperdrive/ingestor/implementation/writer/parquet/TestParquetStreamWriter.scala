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

package za.co.absa.hyperdrive.ingestor.implementation.writer.parquet

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.FlatSpec
import org.scalatest.mockito.MockitoSugar
import za.co.absa.hyperdrive.ingestor.api.manager.StreamManager
import za.co.absa.hyperdrive.testutils.TempDir

class TestParquetStreamWriter extends FlatSpec with MockitoSugar {

  private val tempDir = TempDir.getNew
  private val parquetDestination = new File(tempDir, "test-parquet")
  private val configuration = new Configuration()

  behavior of "ParquetStreamWriter"

  it should "throw on blank destination" in {
    assertThrows[IllegalArgumentException](new ParquetStreamWriter(destination = "  ", Trigger.Once(), None, Map()))
  }

  it should "set format as 'parquet'" in {
    val dataStreamWriter = getDataStreamWriter
    val streamManager = getStreamManager(dataStreamWriter)

    invokeWriter(dataStreamWriter, streamManager, Map())
    verify(dataStreamWriter).format("parquet")
  }

  it should "set Trigger.Once" in {
    val dataStreamWriter = getDataStreamWriter
    val streamManager = getStreamManager(dataStreamWriter)

    invokeWriter(dataStreamWriter, streamManager, Map())
    verify(dataStreamWriter).trigger(Trigger.Once)
  }

  it should "set Trigger.ProcessingTime" in {
    val dataStreamWriter = getDataStreamWriter
    val offsetManager = getStreamManager(dataStreamWriter)

    invokeWriter(dataStreamWriter, offsetManager, Map(), Trigger.ProcessingTime(1L))
    verify(dataStreamWriter).trigger(Trigger.ProcessingTime(1L))
  }

  it should "set output mode as 'append'" in {
    val dataStreamWriter = getDataStreamWriter
    val streamManager = getStreamManager(dataStreamWriter)

    invokeWriter(dataStreamWriter, streamManager, Map())
    verify(dataStreamWriter).outputMode(OutputMode.Append())
  }

  it should "invoke OffsetManager passing DataStreamWriter" in {
    val dataStreamWriter = getDataStreamWriter
    val streamManager = getStreamManager(dataStreamWriter)

    invokeWriter(dataStreamWriter, streamManager, Map())
    verify(streamManager).configure(dataStreamWriter, configuration)
  }

  it should "start DataStreamWriter" in {
    val dataStreamWriter = getDataStreamWriter
    val streamManager = getStreamManager(dataStreamWriter)

    invokeWriter(dataStreamWriter, streamManager, Map())
    verify(dataStreamWriter).start(parquetDestination.getAbsolutePath)
  }

  it should " include extra options in case they exist" in {
    val dataStreamWriter = getDataStreamWriter
    val streamManager = getStreamManager(dataStreamWriter)

    val extraConfs = Map("key.1" -> "value-1", "key.2" -> "value-2")

    invokeWriter(dataStreamWriter, streamManager, extraConfs)
    verify(dataStreamWriter).start(parquetDestination.getAbsolutePath)

    verify(dataStreamWriter).options(extraConfs)
  }

  private def invokeWriter(dataStreamWriter: DataStreamWriter[Row], streamManager: StreamManager,
                           extraOptions: Map[String,String], trigger: Trigger = Trigger.Once()): Unit = {
    val dataFrame = getDataFrame(dataStreamWriter)
    val writer = new ParquetStreamWriter(parquetDestination.getAbsolutePath, trigger, None, extraOptions)
    writer.write(dataFrame, streamManager)
  }

  private def getDataStreamWriter: DataStreamWriter[Row] = {
    val dataStreamWriter = mock[DataStreamWriter[Row]]
    when(dataStreamWriter.trigger(any[Trigger]())).thenReturn(dataStreamWriter)
    when(dataStreamWriter.format(anyString())).thenReturn(dataStreamWriter)
    when(dataStreamWriter.outputMode(any(OutputMode.Append().getClass))).thenReturn(dataStreamWriter)
    when(dataStreamWriter.options(any(classOf[scala.collection.Map[String, String]]))).thenReturn(dataStreamWriter)
    dataStreamWriter
  }

  private def getDataFrame(dataStreamWriter: DataStreamWriter[Row]): DataFrame = {
    val sparkContext = mock[SparkContext]
    when(sparkContext.hadoopConfiguration).thenReturn(configuration)
    val sparkSession = mock[SparkSession]
    when(sparkSession.sparkContext).thenReturn(sparkContext)

    val dataFrame = mock[DataFrame]
    when(dataFrame.writeStream).thenReturn(dataStreamWriter)
    when(dataFrame.sparkSession).thenReturn(sparkSession)
    dataFrame
  }

  private def getStreamManager(dataStreamWriter: DataStreamWriter[Row]): StreamManager = {
    val streamManager = mock[StreamManager]
    when(streamManager.configure(dataStreamWriter, configuration)).thenReturn(dataStreamWriter)
    streamManager
  }
}
