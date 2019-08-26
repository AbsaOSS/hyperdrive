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

package za.co.absa.hyperdrive.reader.factories

import org.apache.commons.configuration2.Configuration
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FlatSpec}
import za.co.absa.hyperdrive.reader.factories.kafka.KafkaStreamReaderFactory

class TestStreamReaderAbstractFactory extends FlatSpec with BeforeAndAfterEach with MockitoSugar {

  private val configStub: Configuration = mock[Configuration]

  behavior of StreamReaderAbstractFactory.getClass.getSimpleName

  override def beforeEach(): Unit = reset(configStub)

  it should "create factory for KafkaStreamReader" in {
    when(configStub.getString(StreamReaderAbstractFactory.componentConfigKey)).thenReturn(KafkaStreamReaderFactory.name)
    assert(KafkaStreamReaderFactory == StreamReaderAbstractFactory.getFactory(configStub))
  }

  it should "throw if reader parameter is invalid" in {
    val invalidFactoryName = "an-invalid-factory-name"
    when(configStub.getString(StreamReaderAbstractFactory.componentConfigKey)).thenReturn(invalidFactoryName)
    val throwable = intercept[IllegalArgumentException](StreamReaderAbstractFactory.getFactory(configStub))
    assert(throwable.getMessage.contains(invalidFactoryName))
  }

  it should "throw if stream reader parameter is absent" in {
    assertThrows[IllegalArgumentException](StreamReaderAbstractFactory.getFactory(configStub))
  }
}