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

package za.co.absa.hyperdrive.ingestor.implementation.reader.parquet

import org.apache.commons.configuration2.BaseConfiguration
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.hyperdrive.ingestor.implementation.reader.parquet.ParquetStreamReader._

class TestParquetStreamReaderObject extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  behavior of ParquetStreamReader.getClass.getSimpleName

  it should "throw on blank source directory" in {
    val config = new BaseConfiguration()

    val throwable = intercept[IllegalArgumentException](ParquetStreamReader(config))

    throwable.getMessage.toLowerCase should include("source")
  }

  it should "instantiate a ParquetStreamReader instance from configurations with default values" in {
    val config = new BaseConfiguration()
    config.addProperty(KeySourceDirectory, "/tmp/source/parquet")
    config.addProperty("reader.parquet.options.key1", "value1")
    config.addProperty("reader.parquet.options.key2", "value2")

    val reader = ParquetStreamReader(config).asInstanceOf[ParquetStreamReader]

    reader.path shouldBe "/tmp/source/parquet"
    reader.extraConfs should contain theSameElementsAs Map(
      "key1" -> "value1",
      "key2" -> "value2"
    )
  }
}
