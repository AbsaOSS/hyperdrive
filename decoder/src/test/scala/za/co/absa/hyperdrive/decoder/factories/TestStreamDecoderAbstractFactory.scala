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

package za.co.absa.hyperdrive.decoder.factories

import org.apache.commons.configuration2.Configuration
import org.scalatest.FlatSpec
import org.scalatest.mockito.MockitoSugar
import org.mockito.Mockito._
import za.co.absa.hyperdrive.decoder.factories.confluent.avro.ConfluentAvroKafkaStreamDecoderFactory


class TestStreamDecoderAbstractFactory extends FlatSpec with MockitoSugar {

  private val configStub = mock[Configuration]

  behavior of StreamDecoderAbstractFactory.getClass.getSimpleName

  it should "create factory for AvroStreamDecoder" in {
    when(configStub.getString(StreamDecoderAbstractFactory.componentConfigKey)).thenReturn(ConfluentAvroKafkaStreamDecoderFactory.name)
    assert(ConfluentAvroKafkaStreamDecoderFactory == StreamDecoderAbstractFactory.getFactory(configStub))
  }

  it should "throw if invalid decoder type is specified" in {
    val invalidDecoderType = "invalid.decoder-type"
    when(configStub.getString(StreamDecoderAbstractFactory.componentConfigKey)).thenReturn(invalidDecoderType)
    val throwable = intercept[IllegalArgumentException](StreamDecoderAbstractFactory.getFactory(configStub))
    assert(throwable.getMessage.toLowerCase.contains(invalidDecoderType))
  }

  it should "throw if no decoder type specified" in {
    assertThrows[IllegalArgumentException](StreamDecoderAbstractFactory.getFactory(configStub))
  }
}
