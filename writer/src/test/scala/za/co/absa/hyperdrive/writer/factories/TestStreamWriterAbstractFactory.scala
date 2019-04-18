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

package za.co.absa.hyperdrive.writer.factories

import org.apache.commons.configuration2.Configuration
import org.mockito.Mockito.{reset, when}
import org.scalatest.{BeforeAndAfterEach, FlatSpec}
import org.scalatest.mockito.MockitoSugar
import za.co.absa.hyperdrive.manager.offset.factories.OffsetManagerAbstractFactory
import za.co.absa.hyperdrive.writer.factories.parquet.ParquetStreamWriterFactory

class TestStreamWriterAbstractFactory extends FlatSpec with BeforeAndAfterEach with MockitoSugar {

  private val configStub = mock[Configuration]

  behavior of OffsetManagerAbstractFactory.getClass.getSimpleName

  override def beforeEach(): Unit = reset(configStub)

  it should "create factory for ParquetStreamWriter" in {
    when(configStub.getString(StreamWriterAbstractFactory.componentConfigKey)).thenReturn(ParquetStreamWriterFactory.name)
    assert(ParquetStreamWriterFactory == StreamWriterAbstractFactory.getFactory(configStub))
  }

  it should "throw if writer parameter is invalid" in {
    val invalidFactoryName = "an-invalid-factory-name"
    when(configStub.getString(StreamWriterAbstractFactory.componentConfigKey)).thenReturn(invalidFactoryName)
    val throwable = intercept[IllegalArgumentException](StreamWriterAbstractFactory.getFactory(configStub))
    assert(throwable.getMessage.contains(invalidFactoryName))
  }

  it should "throw if offset manager parameter is absent" in {
    assertThrows[IllegalArgumentException](StreamWriterAbstractFactory.getFactory(configStub))
  }
}
