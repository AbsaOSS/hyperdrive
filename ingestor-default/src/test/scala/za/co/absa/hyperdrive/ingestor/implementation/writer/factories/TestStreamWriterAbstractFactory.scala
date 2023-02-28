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

package za.co.absa.hyperdrive.ingestor.implementation.writer.factories

import org.apache.commons.configuration2.BaseConfiguration
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.hyperdrive.ingestor.api.writer.StreamWriterCommonAttributes
import za.co.absa.hyperdrive.ingestor.implementation.writer.parquet.ParquetStreamWriter
import za.co.absa.hyperdrive.ingestor.implementation.writer.parquet.ParquetStreamWriter.{KEY_DESTINATION_DIRECTORY, KEY_EXTRA_CONFS_ROOT}

class TestStreamWriterAbstractFactory extends AnyFlatSpec with BeforeAndAfterEach with MockitoSugar with Matchers {

  behavior of StreamWriterAbstractFactory.getClass.getSimpleName

  it should "create factory for ParquetStreamWriter" in {
    val config = new BaseConfiguration()
    config.addProperty(StreamWriterAbstractFactory.componentConfigKey, ParquetStreamWriter.getClass.getName)
    config.addProperty(KEY_DESTINATION_DIRECTORY, "/tmp/parquet")
    config.addProperty(KEY_EXTRA_CONFS_ROOT, "")
    config.addProperty(StreamWriterCommonAttributes.keyCheckpointBaseLocation, "/tmp/checkpoint")

    assert(StreamWriterAbstractFactory.build(config).isInstanceOf[ParquetStreamWriter])
  }

  it should "throw if writer parameter is invalid" in {
    val invalidFactoryName = "an-invalid-factory-name"
    val config = new BaseConfiguration()
    config.addProperty(StreamWriterAbstractFactory.componentConfigKey, invalidFactoryName)

    val throwable = intercept[IllegalArgumentException](StreamWriterAbstractFactory.build(config))
    throwable.getMessage should include(invalidFactoryName)
  }

  it should "throw if writer parameter is absent" in {
    assertThrows[IllegalArgumentException](StreamWriterAbstractFactory.build(new BaseConfiguration()))
  }
}
