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

package za.co.absa.hyperdrive.manager.offset.factories.checkpoint

import org.apache.commons.configuration2.Configuration
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FlatSpec}
import org.mockito.Mockito._
import za.co.absa.hyperdrive.manager.offset.impl.checkpoint.CheckpointOffsetManager
import za.co.absa.hyperdrive.shared.configurations.ConfigurationsKeys.CheckpointOffsetManagerKeys._

class TestCheckpointOffsetManagerFactory extends FlatSpec with BeforeAndAfterEach with MockitoSugar {

  private val topic = "topic"
  private val checkpointLocation = "/tmp/checkpoint/location"

  private val configStub = mock[Configuration]

  override def beforeEach(): Unit = reset(configStub)

  behavior of CheckpointOffsetManagerFactory.getClass.getSimpleName

  it should "throw on blank topic" in {
    stubCheckpointLocation()

    val throwable = intercept[IllegalArgumentException](CheckpointOffsetManagerFactory.build(configStub))
    assert(throwable.getMessage.toLowerCase.contains("topic"))
  }

  it should "throw on blank checkpoint location" in {
    stubTopic()

    val throwable = intercept[IllegalArgumentException](CheckpointOffsetManagerFactory.build(configStub))
    assert(throwable.getMessage.toLowerCase.contains("location"))
  }

  it should "create checkpoint offset manager" in {
    stubTopic()
    stubCheckpointLocation()

    val manager = CheckpointOffsetManagerFactory.build(configStub).asInstanceOf[CheckpointOffsetManager]
    assert(topic == manager.topic)
    assert(checkpointLocation == manager.checkpointBaseLocation)
  }

  private def stubProperty(key: String, value: String): Unit = when(configStub.getString(key)).thenReturn(value)

  private def stubTopic(): Unit = stubProperty(KEY_TOPIC, topic)

  private def stubCheckpointLocation(): Unit = stubProperty(KEY_CHECKPOINT_BASE_LOCATION, checkpointLocation)
}
