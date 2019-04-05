/*
 *
 * Copyright 2019 ABSA Group Limited
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package za.co.absa.hyperdrive.writer.impl

import java.io.File

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, Trigger}
import org.scalatest.FlatSpec
import org.scalatest.mockito.MockitoSugar
import za.co.absa.hyperdrive.manager.offset.OffsetManager
import za.co.absa.hyperdrive.shared.utils.TempDir
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._

class TestParquetStreamWriter extends FlatSpec with MockitoSugar {

  private val tempDir = TempDir.getNew
  private val parquetDestination = new File(tempDir, "test-parquet")

  behavior of "ParquetStreamWriter"

  it should "throw on blank destination" in {
    assertThrows[IllegalArgumentException](new ParquetStreamWriter(destination = null, None))
    assertThrows[IllegalArgumentException](new ParquetStreamWriter(destination = "  ", None))
  }

  it should "throw on null DataFrame" in {
    val dataStreamWriter = mock[DataStreamWriter[Row]]

    val offsetManager = mock[OffsetManager]
    when(offsetManager.configureOffsets(dataStreamWriter)).thenReturn(dataStreamWriter)

    val writer = new ParquetStreamWriter(parquetDestination.getAbsolutePath, None)
    assertThrows[IllegalArgumentException](writer.write(dataFrame = null, offsetManager))
  }

  it should "throw on null OffsetManager" in {
    val dataStreamWriter = mock[DataStreamWriter[Row]]

    val dataFrame = mock[DataFrame]
    when(dataFrame.writeStream).thenReturn(dataStreamWriter)

    val writer = new ParquetStreamWriter(parquetDestination.getAbsolutePath, None)
    assertThrows[IllegalArgumentException](writer.write(dataFrame, offsetManager = null))
  }

  it should "set format as 'parquet'" in {
    val dataStreamWriter = getDataStreamWriter
    val offsetManager = getOffsetManager(dataStreamWriter)

    invokeWriter(dataStreamWriter, offsetManager, None)
    verify(dataStreamWriter).format("parquet")
  }

  it should "set Trigger.Once" in {
    val dataStreamWriter = getDataStreamWriter
    val offsetManager = getOffsetManager(dataStreamWriter)

    invokeWriter(dataStreamWriter, offsetManager, None)
    verify(dataStreamWriter).trigger(Trigger.Once)
  }

  it should "set output mode as 'append'" in {
    val dataStreamWriter = getDataStreamWriter
    val offsetManager = getOffsetManager(dataStreamWriter)

    invokeWriter(dataStreamWriter, offsetManager, None)
    verify(dataStreamWriter).outputMode(OutputMode.Append())
  }

  it should "invoke OffsetManager passing DataStreamWriter" in {
    val dataStreamWriter = getDataStreamWriter
    val offsetManager = getOffsetManager(dataStreamWriter)

    invokeWriter(dataStreamWriter, offsetManager, None)
    verify(offsetManager).configureOffsets(dataStreamWriter)
  }

  it should "start DataStreamWriter" in {
    val dataStreamWriter = getDataStreamWriter
    val offsetManager = getOffsetManager(dataStreamWriter)

    invokeWriter(dataStreamWriter, offsetManager, None)
    verify(dataStreamWriter).start(parquetDestination.getAbsolutePath)
  }

  it should " include extra options in case they exist" in {
    val dataStreamWriter = getDataStreamWriter
    val offsetManager = getOffsetManager(dataStreamWriter)

    val extraConfs = Map("key.1" -> "value-1", "key.2" -> "value-2")

    invokeWriter(dataStreamWriter, offsetManager, Some(extraConfs))
    verify(dataStreamWriter).start(parquetDestination.getAbsolutePath)

    extraConfs.foreach {
      case (key: String, value: String) => verify(dataStreamWriter).option(key, value)
    }
  }

  private def invokeWriter(dataStreamWriter: DataStreamWriter[Row], offsetManager: OffsetManager, extraOptions: Option[Map[String,String]]): Unit = {
    val dataFrame = getDataFrame(dataStreamWriter)
    val writer = new ParquetStreamWriter(parquetDestination.getAbsolutePath, extraOptions)
    writer.write(dataFrame, offsetManager)
  }

  private def getDataStreamWriter: DataStreamWriter[Row] = {
    val dataStreamWriter = mock[DataStreamWriter[Row]]
    when(dataStreamWriter.trigger(any(Trigger.Once().getClass))).thenReturn(dataStreamWriter)
    when(dataStreamWriter.format(anyString())).thenReturn(dataStreamWriter)
    when(dataStreamWriter.outputMode(any(OutputMode.Append().getClass))).thenReturn(dataStreamWriter)
    when(dataStreamWriter.option(anyString(), anyString())).thenReturn(dataStreamWriter)
    dataStreamWriter
  }

  private def getDataFrame(dataStreamWriter: DataStreamWriter[Row]): DataFrame = {
    val dataFrame = mock[DataFrame]
    when(dataFrame.writeStream).thenReturn(dataStreamWriter)
    dataFrame
  }

  private def getOffsetManager(dataStreamWriter: DataStreamWriter[Row]): OffsetManager = {
    val offsetManager = mock[OffsetManager]
    when(offsetManager.configureOffsets(dataStreamWriter)).thenReturn(dataStreamWriter)
    offsetManager
  }
}
