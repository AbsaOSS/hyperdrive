/*
 * Copyright 2018-2019 ABSA Group Limited
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

package za.co.absa.hyperdrive.ingestor.implementation.finalizer.factories

import org.apache.commons.configuration2.Configuration
import org.mockito.Mockito._
import org.scalatest.FlatSpec
import org.scalatest.mockito.MockitoSugar
import za.co.absa.hyperdrive.ingestor.implementation.finalizer.noop.NoOpFinalizer

class TestIngestionFinalizerAbstractFactory extends FlatSpec with MockitoSugar {

  private val configStub = mock[Configuration]

  behavior of IngestionFinalizerAbstractFactory.getClass.getSimpleName

  it should "create IngestionFinalizer" in {
    when(configStub.getString(IngestionFinalizerAbstractFactory.componentConfigKey)).thenReturn(NoOpFinalizer.getClass.getName)

    assert(IngestionFinalizerAbstractFactory.build(configStub).isInstanceOf[NoOpFinalizer])
  }

  it should "throw if invalid decoder type is specified" in {
    val invalidDecoderType = "invalid.decoder-type"
    when(configStub.getString(IngestionFinalizerAbstractFactory.componentConfigKey)).thenReturn(invalidDecoderType)
    val throwable = intercept[IllegalArgumentException](IngestionFinalizerAbstractFactory.build(configStub))
    assert(throwable.getMessage.toLowerCase.contains(invalidDecoderType))
  }

  it should "throw if no decoder type specified" in {
    assertThrows[IllegalArgumentException](IngestionFinalizerAbstractFactory.build(configStub))
  }
}
