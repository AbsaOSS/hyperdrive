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

import org.apache.commons.configuration2.{BaseConfiguration, Configuration}
import org.scalatest.{BeforeAndAfterEach, FlatSpec}
import za.co.absa.hyperdrive.shared.configurations.ConfigurationsKeys.ParquetStreamWriterKeys._

class TestParquetStreamWriterObject extends FlatSpec with BeforeAndAfterEach {

  private val configStub: Configuration = new BaseConfiguration()

  private val destinationDirectory = "/tmp/destination/parquet"
  private val extraConfs = Map("key.1" -> "value.1", "key.2" -> "value.2")

  override def beforeEach(): Unit = configStub.clear()

  behavior of ParquetStreamWriter.getClass.getSimpleName

  it should "throw on blank destination directory" in {
    val throwable = intercept[IllegalArgumentException](ParquetStreamWriter(configStub))
    assert(throwable.getMessage.toLowerCase.contains("destination"))
  }

  it should "instantiate a ParquetStreamWriter instance from configurations" in {
    stubDestinationDirectory()
    stubExtraConfs()

    val writer = ParquetStreamWriter(configStub).asInstanceOf[ParquetStreamWriter]
    assert(destinationDirectory == writer.getDestination)
    assert(extraConfs.toSet.diff(writer.extraConfOptions.get.toSet).isEmpty)
  }

  it should "throw if an extra option is malformed" in {
    stubDestinationDirectory()

    val writer = ParquetStreamWriter(configStub).asInstanceOf[ParquetStreamWriter]
    assert(destinationDirectory == writer.getDestination)
    assert(writer.extraConfOptions.isEmpty)
  }

  it should "not throw on absent extra configurations" in {
    stubDestinationDirectory()
    stubExtraConfs()
    stubStringConfig(s"$KEY_EXTRA_CONFS_ROOT.wrong.conf","only.key=")

    assertThrows[IllegalArgumentException](ParquetStreamWriter(configStub))
  }

  private def stubStringConfig(key: String, value: String): Unit = configStub.addProperty(key, value)

  private def stubDestinationDirectory(): Unit = stubStringConfig(KEY_DESTINATION_DIRECTORY, destinationDirectory)

  private def stubExtraConfs(): Unit = {
    extraConfs
      .zipWithIndex
      .foreach {
        case((key,value),index) => stubStringConfig(s"$KEY_EXTRA_CONFS_ROOT.$index", s"$key=$value")
      }
  }
}
