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

package za.co.absa.hyperdrive.ingestor.implementation.writer.parquet

import org.apache.commons.configuration2.BaseConfiguration
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import za.co.absa.hyperdrive.shared.configurations.ConfigurationsKeys.ParquetStreamWriterKeys._

class TestParquetStreamWriterObject extends FlatSpec with Matchers with BeforeAndAfterEach {

  behavior of ParquetStreamWriter.getClass.getSimpleName

  it should "throw on blank destination directory" in {
    val config = new BaseConfiguration()

    val throwable = intercept[IllegalArgumentException](ParquetStreamWriter(config))

    throwable.getMessage.toLowerCase should include("destination")
  }

  it should "instantiate a ParquetStreamWriter instance from configurations" in {
    val config = new BaseConfiguration()
    config.addProperty(KEY_DESTINATION_DIRECTORY, "/tmp/destination/parquet")
    config.addProperty("writer.parquet.options.key1", "value1")
    config.addProperty("writer.parquet.options.key2", "value2")

    val writer = ParquetStreamWriter(config).asInstanceOf[ParquetStreamWriter]

    writer.getDestination shouldBe "/tmp/destination/parquet"
    writer.extraConfOptions should contain theSameElementsAs Map(
      "key1" -> "value1",
      "key2" -> "value2"
    )
  }

  it should "not throw on absent extra configurations" in {
    val config = new BaseConfiguration()
    config.addProperty(KEY_DESTINATION_DIRECTORY, "/tmp/destination/parquet")

    val writer = ParquetStreamWriter(config).asInstanceOf[ParquetStreamWriter]

    writer.getDestination shouldBe "/tmp/destination/parquet"
    writer.extraConfOptions shouldBe Map()
  }
}
