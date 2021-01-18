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

package za.co.absa.hyperdrive.ingestor.implementation.writer.mongodb

import org.apache.commons.configuration2.BaseConfiguration
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import za.co.absa.hyperdrive.ingestor.api.writer.StreamWriterCommonAttributes
import za.co.absa.hyperdrive.ingestor.implementation.writer.mongodb.MongoDbStreamWriter.KEY_URI

class TestMongoDbStreamWriterObject extends FlatSpec with Matchers with BeforeAndAfterEach {

  behavior of MongoDbStreamWriter.getClass.getSimpleName

  it should "throw on an absent MongoDB URI" in {
    val config = new BaseConfiguration()
    config.addProperty(StreamWriterCommonAttributes.keyCheckpointBaseLocation, "/tmp/checkpoint")

    val throwable = intercept[IllegalArgumentException](MongoDbStreamWriter(config))

    throwable.getMessage.toLowerCase should include("is not specified")
  }

  it should "throw on an incorrect MongoDB URI" in {
    val config = new BaseConfiguration()
    config.addProperty(KEY_URI, "http://127.0.0.1")
    config.addProperty(StreamWriterCommonAttributes.keyCheckpointBaseLocation, "/tmp/checkpoint")

    val throwable = intercept[IllegalArgumentException](MongoDbStreamWriter(config))

    throwable.getMessage.toLowerCase should include("invalid mongodb uri")
  }

  it should "instantiate a MongoDbStreamWriter instance from configurations" in {
    val config = new BaseConfiguration()
    config.addProperty(KEY_URI, "mongodb://127.0.0.1")
    config.addProperty("writer.mongodb.options.key1", "value1")
    config.addProperty("writer.mongodb.options.key2", "value2")
    config.addProperty(StreamWriterCommonAttributes.keyCheckpointBaseLocation, "/tmp/checkpoint")

    val writer = MongoDbStreamWriter(config).asInstanceOf[MongoDbStreamWriter]

    writer.getDestination shouldBe "mongodb://127.0.0.1"
    writer.extraConfOptions should contain theSameElementsAs Map(
      "key1" -> "value1",
      "key2" -> "value2"
    )
  }

  it should "not throw on absent extra configurations" in {
    val config = new BaseConfiguration()
    config.addProperty(KEY_URI, "mongodb://127.0.0.1")
    config.addProperty(StreamWriterCommonAttributes.keyCheckpointBaseLocation, "/tmp/checkpoint")

    val writer = MongoDbStreamWriter(config).asInstanceOf[MongoDbStreamWriter]

    writer.getDestination shouldBe "mongodb://127.0.0.1"
    writer.extraConfOptions shouldBe Map()
  }

}
