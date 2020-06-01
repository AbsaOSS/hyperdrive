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

package za.co.absa.hyperdrive.ingestor.implementation.manager.checkpoint


import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter}
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FlatSpec}
import za.co.absa.hyperdrive.ingestor.implementation.manager.checkpoint.CheckpointOffsetManagerProps._
import za.co.absa.hyperdrive.shared.configurations.ConfigurationsKeys.KafkaStreamReaderKeys.WORD_STARTING_OFFSETS
import za.co.absa.commons.io.TempDirectory

class TestCheckpointOffsetManager extends FlatSpec with BeforeAndAfterEach with  MockitoSugar {

  private var tempDir = TempDirectory()
  private val validConfiguration = new Configuration()

  override def beforeEach: Unit = tempDir = TempDirectory()

  override def afterEach: Unit = tempDir.delete()

  behavior of "CheckpointingOffsetManager"

  it should "set offsets to earliest if no checkpoint location exists" in {
    val dataStreamReader = mock[DataStreamReader]
    val nonExistent = tempDir.path.resolve("non-existent")
    val manager = new CheckpointOffsetManager(checkpointLocation = nonExistent.toUri.getPath, None)
    manager.configure(dataStreamReader, validConfiguration)

    verify(dataStreamReader).option(WORD_STARTING_OFFSETS, STARTING_OFFSETS_EARLIEST)
  }

  it should "set offsets to user-defined property if no checkpoint location exists" in {
    val dataStreamReader = mock[DataStreamReader]
    val nonExistent = tempDir.path.resolve("non-existent")
    val manager = new CheckpointOffsetManager(checkpointLocation = nonExistent.toUri.getPath, Some("latest"))
    manager.configure(dataStreamReader, validConfiguration)

    verify(dataStreamReader).option(WORD_STARTING_OFFSETS, "latest")
  }

  it should "not set offsets if a checkpoint location exists" in {
    val dataStreamReader = mock[DataStreamReader]
    val manager = new CheckpointOffsetManager(checkpointLocation = tempDir.path.toAbsolutePath.toString, Some("latest"))
    manager.configure(dataStreamReader, validConfiguration)

    verify(dataStreamReader, never()).option(WORD_STARTING_OFFSETS, STARTING_OFFSETS_EARLIEST) // should not set offset, since checkpoint location exists
  }

  it should "set checkpoint location" in {
    val dataStreamWriter = mock[DataStreamWriter[Row]]
    val manager = new CheckpointOffsetManager(checkpointLocation = tempDir.path.toAbsolutePath.toString, None)
    manager.configure(dataStreamWriter, validConfiguration)

    val expectedCheckpointLocationTopic = tempDir.path.toAbsolutePath.toString
    verify(dataStreamWriter).option(CHECKPOINT_LOCATION_KEY, expectedCheckpointLocationTopic)
  }
}
