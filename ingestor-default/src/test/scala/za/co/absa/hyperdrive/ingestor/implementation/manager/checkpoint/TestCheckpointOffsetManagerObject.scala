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

import org.apache.commons.configuration2.Configuration
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FlatSpec}
import za.co.absa.hyperdrive.shared.configurations.ConfigurationsKeys.CheckpointOffsetManagerKeys._
import za.co.absa.hyperdrive.shared.configurations.ConfigurationsKeys.KafkaStreamReaderKeys.KEY_STARTING_OFFSETS

class TestCheckpointOffsetManagerObject extends FlatSpec with BeforeAndAfterEach with MockitoSugar {

  private val checkpointLocation = "/tmp/checkpoint/location"

  private val configStub = mock[Configuration]

  override def beforeEach(): Unit = reset(configStub)

  behavior of CheckpointOffsetManager.getClass.getSimpleName

  it should "throw on blank checkpoint location" in {
    val throwable = intercept[IllegalArgumentException](CheckpointOffsetManager(configStub))
    assert(throwable.getMessage.toLowerCase.contains("location"))
  }

  it should "create checkpoint offset manager" in {
    stubCheckpointLocation()

    val manager = CheckpointOffsetManager(configStub).asInstanceOf[CheckpointOffsetManager]
    assert(checkpointLocation == manager.checkpointLocation)
    assert(manager.startingOffsets.isEmpty)
  }

  it should "use the given startingOffsets value" in {
    stubCheckpointLocation()
    stubProperty(KEY_STARTING_OFFSETS, "latest")

    val manager = CheckpointOffsetManager(configStub).asInstanceOf[CheckpointOffsetManager]
    assert(checkpointLocation == manager.checkpointLocation)
    assert("latest" == manager.startingOffsets.get)
  }

  private def stubProperty(key: String, value: String): Unit = when(configStub.getString(key)).thenReturn(value)

  private def stubCheckpointLocation(): Unit = stubProperty(KEY_CHECKPOINT_BASE_LOCATION, checkpointLocation)
}
