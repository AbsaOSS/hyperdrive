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
import za.co.absa.hyperdrive.ingestor.implementation.mongodbutils.MongoDbFixture
import za.co.absa.hyperdrive.ingestor.implementation.testutils.MemoryStreamFixture
import za.co.absa.hyperdrive.ingestor.implementation.writer.mongodb.MongoDbStreamWriter.KEY_URI

class TestMongoDbStreamWriter2 extends FlatSpec with SparkTestBase with MongoDbFixture with MemoryStreamFixture {

  private val baseDir = TempDirectory("TestMongodbStreamWriter").deleteOnExit()

  behavior of "MongoDbStreamWriter"

  it should "write data to MongoDB" in {
    import spark.implicits._

    // given
    val inputData = Range(0, 100).toDF

    val config = new BaseConfiguration()
    config.addProperty(KEY_URI, s"$connectionString/testdb/testcollection")
    config.addProperty(StreamWriterCommonAttributes.keyCheckpointBaseLocation, s"$baseDir/checkpoint1")
    val writer = MongoDbStreamWriter(config).asInstanceOf[MongoDbStreamWriter]

    withStreamingData(inputData) { streamDf =>
      // (!) Uncomment to use the actual MongoDB writer
      //val sink = writer.write(streamDf)
      val sink = streamDf
        .writeStream
        .queryName("tmp_table")
        .outputMode("append")
        .format("memory")
        .start()

      sink.processAllAvailable()
      sink.stop()
    }

  }

}
