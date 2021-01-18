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
import org.scalatest.FlatSpec
import za.co.absa.commons.io.TempDirectory
import za.co.absa.commons.spark.SparkTestBase
import za.co.absa.hyperdrive.ingestor.api.writer.StreamWriterCommonAttributes
import za.co.absa.hyperdrive.ingestor.implementation.mongodbutils.{MongoDbFixture, ScalaMongoImplicits}
import za.co.absa.hyperdrive.ingestor.implementation.testutils.MemoryStreamFixture
import za.co.absa.hyperdrive.ingestor.implementation.writer.mongodb.MongoDbStreamWriter.KEY_URI

class TestMongoDbStreamWriterIntegration extends FlatSpec with SparkTestBase with MongoDbFixture with MemoryStreamFixture {
  import spark.implicits._
  import ScalaMongoImplicits._

  private val baseDir = TempDirectory("TestMongodbStreamWriter").deleteOnExit()

  behavior of "MongoDbStreamWriter"

  it should "write data to MongoDB" in {
    // given
    val inputData = Range(0, 100).toDF
    val collectionName = "testcollection"

    db.createCollection(collectionName)

    val config = new BaseConfiguration()
    config.addProperty(KEY_URI, s"$connectionString/$dbName.$collectionName")
    config.addProperty(StreamWriterCommonAttributes.keyCheckpointBaseLocation, s"$baseDir/checkpoint1")
    val writer = MongoDbStreamWriter(config).asInstanceOf[MongoDbStreamWriter]

    withStreamingData(inputData) { streamDf =>
      val sink = writer.write(streamDf)
      sink.processAllAvailable()
      sink.stop()

      val count = db.getCollection(collectionName)
        .countDocuments()
        .execute()

      assert(count == 100)
    }
  }

}
