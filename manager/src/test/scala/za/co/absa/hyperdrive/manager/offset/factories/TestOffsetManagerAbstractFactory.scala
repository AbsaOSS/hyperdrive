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

package za.co.absa.hyperdrive.manager.offset.factories

import org.apache.commons.configuration2.Configuration
import org.mockito.Mockito.when
import org.scalatest.{BeforeAndAfterEach, FlatSpec}
import org.scalatest.mockito.MockitoSugar
import org.mockito.Mockito._
import za.co.absa.hyperdrive.manager.offset.factories.checkpoint.CheckpointOffsetManagerFactory

class TestOffsetManagerAbstractFactory extends FlatSpec with BeforeAndAfterEach with MockitoSugar {

  private val configStub = mock[Configuration]

  behavior of OffsetManagerAbstractFactory.getClass.getSimpleName

  override def beforeEach(): Unit = reset(configStub)

  it should "create factory for CheckpointOffsetManager" in {
    when(configStub.getString(OffsetManagerAbstractFactory.componentConfigKey)).thenReturn(CheckpointOffsetManagerFactory.name)
    assert(CheckpointOffsetManagerFactory == OffsetManagerAbstractFactory.getFactory(configStub))
  }

  it should "throw if manager parameter is invalid" in {
    val invalidFactoryName = "an-invalid-factory-name"
    when(configStub.getString(OffsetManagerAbstractFactory.componentConfigKey)).thenReturn(invalidFactoryName)
    val throwable = intercept[IllegalArgumentException](OffsetManagerAbstractFactory.getFactory(configStub))
    assert(throwable.getMessage.contains(invalidFactoryName))
  }

  it should "throw if offset manager parameter is absent" in {
    assertThrows[IllegalArgumentException](OffsetManagerAbstractFactory.getFactory(configStub))
  }
}
