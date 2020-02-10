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
    assertThrows[IllegalArgumentException](new ParquetStreamWriter(destination = null, Map()))
    assertThrows[IllegalArgumentException](new ParquetStreamWriter(destination = "  ", Map()))
  }

  it should "throw on null DataFrame" in {
    val dataStreamWriter = mock[DataStreamWriter[Row]]

    val offsetManager = mock[StreamManager]
    when(offsetManager.configure(dataStreamWriter, null)).thenReturn(dataStreamWriter)

    val writer = new ParquetStreamWriter(parquetDestination.getAbsolutePath, Map())
    assertThrows[IllegalArgumentException](writer.write(dataFrame = null, offsetManager))
  }

  it should "throw on null OffsetManager" in {
    val dataStreamWriter = mock[DataStreamWriter[Row]]

    val dataFrame = mock[DataFrame]
    when(dataFrame.writeStream).thenReturn(dataStreamWriter)

    val writer = new ParquetStreamWriter(parquetDestination.getAbsolutePath, Map())
    assertThrows[IllegalArgumentException](writer.write(dataFrame, offsetManager = null))
  }

  it should "set format as 'parquet'" in {
    val dataStreamWriter = getDataStreamWriter
    val offsetManager = getOffsetManager(dataStreamWriter)

    invokeWriter(dataStreamWriter, offsetManager, Map())
    verify(dataStreamWriter).format("parquet")
  }

  it should "set Trigger.Once" in {
    val dataStreamWriter = getDataStreamWriter
    val offsetManager = getOffsetManager(dataStreamWriter)

    invokeWriter(dataStreamWriter, offsetManager, Map())
    verify(dataStreamWriter).trigger(Trigger.Once)
  }

  it should "set output mode as 'append'" in {
    val dataStreamWriter = getDataStreamWriter
    val offsetManager = getOffsetManager(dataStreamWriter)

    invokeWriter(dataStreamWriter, offsetManager, Map())
    verify(dataStreamWriter).outputMode(OutputMode.Append())
  }

  it should "invoke OffsetManager passing DataStreamWriter" in {
    val dataStreamWriter = getDataStreamWriter
    val offsetManager = getOffsetManager(dataStreamWriter)

    invokeWriter(dataStreamWriter, offsetManager, Map())
    verify(offsetManager).configure(dataStreamWriter, configuration)
  }

  it should "start DataStreamWriter" in {
    val dataStreamWriter = getDataStreamWriter
    val offsetManager = getOffsetManager(dataStreamWriter)

    invokeWriter(dataStreamWriter, offsetManager, Map())
    verify(dataStreamWriter).start(parquetDestination.getAbsolutePath)
  }

  it should " include extra options in case they exist" in {
    val dataStreamWriter = getDataStreamWriter
    val offsetManager = getOffsetManager(dataStreamWriter)

    val extraConfs = Map("key.1" -> "value-1", "key.2" -> "value-2")

    invokeWriter(dataStreamWriter, offsetManager, extraConfs)
    verify(dataStreamWriter).start(parquetDestination.getAbsolutePath)

    verify(dataStreamWriter).options(extraConfs)
  }

  private def invokeWriter(dataStreamWriter: DataStreamWriter[Row], offsetManager: StreamManager, extraOptions: Map[String,String]): Unit = {
    val dataFrame = getDataFrame(dataStreamWriter)
    val writer = new ParquetStreamWriter(parquetDestination.getAbsolutePath, extraOptions)
    writer.write(dataFrame, offsetManager)
  }

  private def getDataStreamWriter: DataStreamWriter[Row] = {
    val dataStreamWriter = mock[DataStreamWriter[Row]]
    when(dataStreamWriter.trigger(any(Trigger.Once().getClass))).thenReturn(dataStreamWriter)
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

  private def getOffsetManager(dataStreamWriter: DataStreamWriter[Row]): StreamManager = {
    val offsetManager = mock[StreamManager]
    when(offsetManager.configure(dataStreamWriter, configuration)).thenReturn(dataStreamWriter)
    offsetManager
  }
}
