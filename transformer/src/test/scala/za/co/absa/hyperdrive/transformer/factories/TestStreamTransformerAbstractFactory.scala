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

package za.co.absa.hyperdrive.transformer.factories

import org.apache.commons.configuration2.Configuration
import org.scalatest.{BeforeAndAfterEach, FlatSpec}
import org.scalatest.mockito.MockitoSugar
import org.mockito.Mockito._
import za.co.absa.hyperdrive.transformer.factories.select.ColumnSelectorStreamTransformerFactory

class TestStreamTransformerAbstractFactory extends FlatSpec with BeforeAndAfterEach with MockitoSugar {

  private val configuration:Configuration = mock[Configuration]

  override def beforeEach(): Unit = reset(configuration)

  behavior of StreamTransformerAbstractFactory.getClass.getSimpleName

  it should "create factory for ColumnSelectorStreamTransformer" in {
    when(configuration.getString(StreamTransformerAbstractFactory.componentConfigKey)).thenReturn(ColumnSelectorStreamTransformerFactory.name)
    assert(StreamTransformerAbstractFactory.getFactory(configuration) == ColumnSelectorStreamTransformerFactory)
  }

  it should "throw if data transformer parameter is invalid" in {
    val invalidFactoryName = "an-invalid-factory-name"
    when(configuration.getString(StreamTransformerAbstractFactory.componentConfigKey)).thenReturn(invalidFactoryName)
    val throwable = intercept[IllegalArgumentException](StreamTransformerAbstractFactory.getFactory(configuration))
    assert(throwable.getMessage.contains(invalidFactoryName))
  }

  it should "throw if data transformer parameter is absent" in {
    assertThrows[IllegalArgumentException](StreamTransformerAbstractFactory.getFactory(configuration))
  }
}
