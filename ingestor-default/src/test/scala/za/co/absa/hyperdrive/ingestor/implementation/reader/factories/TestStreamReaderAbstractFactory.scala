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

package za.co.absa.hyperdrive.ingestor.implementation.reader.factories

import org.apache.commons.configuration2.BaseConfiguration
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.commons.io.TempDirectory
import za.co.absa.hyperdrive.ingestor.api.writer.StreamWriterCommonAttributes
import za.co.absa.hyperdrive.ingestor.implementation.reader.kafka.KafkaStreamReader
import za.co.absa.hyperdrive.ingestor.implementation.reader.kafka.KafkaStreamReader.{KEY_BROKERS, KEY_TOPIC}

class TestStreamReaderAbstractFactory extends AnyFlatSpec with BeforeAndAfterEach with MockitoSugar with Matchers {

  behavior of StreamReaderAbstractFactory.getClass.getSimpleName
  private val baseDir = TempDirectory("TestStreamReaderAbstractFactory").deleteOnExit()

  it should "create KafkaStreamReader" in {
    val config = new BaseConfiguration()
    config.addProperty(StreamReaderAbstractFactory.componentConfigKey, KafkaStreamReader.getClass.getName)
    config.addProperty(KEY_TOPIC, "topic")
    config.addProperty(KEY_BROKERS, "http://localhost:9092")
    config.addProperty(StreamWriterCommonAttributes.keyCheckpointBaseLocation, baseDir.path.toAbsolutePath.toString)

    assert(StreamReaderAbstractFactory.build(config).isInstanceOf[KafkaStreamReader])
  }

  it should "throw if reader parameter is invalid" in {
    val invalidFactoryName = "an-invalid-factory-name"
    val config = new BaseConfiguration()
    config.addProperty(StreamReaderAbstractFactory.componentConfigKey, invalidFactoryName)

    val throwable = intercept[IllegalArgumentException](StreamReaderAbstractFactory.build(config))

    throwable.getMessage should include(invalidFactoryName)
  }

  it should "throw if stream reader parameter is absent" in {
    assertThrows[IllegalArgumentException](StreamReaderAbstractFactory.build(new BaseConfiguration()))
  }
}
