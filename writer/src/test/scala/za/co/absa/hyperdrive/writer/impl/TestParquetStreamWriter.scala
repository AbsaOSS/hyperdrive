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
import org.apache.spark.sql.streaming.DataStreamWriter
import org.scalatest.FlatSpec
import org.scalatest.mockito.MockitoSugar
import za.co.absa.hyperdrive.manager.offset.OffsetManager
import za.co.absa.hyperdrive.shared.utils.TempDir
import org.mockito.Mockito._

class TestParquetStreamWriter extends FlatSpec with MockitoSugar {

  private val tempDir = TempDir.getNew
  private val parquetDestination = new File(tempDir, "test-parquet")

  behavior of "ParquetStreamWriter"

  it should "throw on blank destination" in {
    assertThrows[IllegalArgumentException](new ParquetStreamWriter(destination = null))
    assertThrows[IllegalArgumentException](new ParquetStreamWriter(destination = "  "))
  }

  it should "throw on null DataFrame" in {
    val dataStreamWriter = mock[DataStreamWriter[Row]]

    val offsetManager = mock[OffsetManager]
    when(offsetManager.configureOffsets(dataStreamWriter)).thenReturn(dataStreamWriter)

    val writer = new ParquetStreamWriter(parquetDestination.getAbsolutePath)
    assertThrows[IllegalArgumentException](writer.write(dataFrame = null, offsetManager))
  }

  it should "throw on null OffsetManager" in {
    val dataStreamWriter = mock[DataStreamWriter[Row]]

    val dataFrame = mock[DataFrame]
    when(dataFrame.writeStream).thenReturn(dataStreamWriter)

    val writer = new ParquetStreamWriter(parquetDestination.getAbsolutePath)
    assertThrows[IllegalArgumentException](writer.write(dataFrame, offsetManager = null))
  }

  it should "set format as 'parquet'" in {

  }

  it should "set Trigger.Once" in {

  }

  it should "set output mode as 'append'" in {

  }

  it should "invoke OffsetManager passing DataStreamWriter" in {

  }

  it should "start DataStreamWriter" in {

  }
}
