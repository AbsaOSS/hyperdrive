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
import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import za.co.absa.commons.io.TempDirectory
import za.co.absa.spark.commons.test.SparkTestBase
import za.co.absa.hyperdrive.ingestor.api.writer.{StreamWriter, StreamWriterCommonAttributes}
import za.co.absa.hyperdrive.ingestor.implementation.testutils.mongodb.ScalaMongoImplicits
import za.co.absa.hyperdrive.ingestor.implementation.testutils.MemoryStreamFixture
import za.co.absa.hyperdrive.ingestor.implementation.testutils.mongodb.MongoDbFixture
import za.co.absa.hyperdrive.ingestor.implementation.writer.mongodb.MongoDbStreamWriter.{KEY_COLLECTION, KEY_DATABASE, KEY_URI}

class TestMongoDbStreamWriterIntegration extends AnyFlatSpec with SparkTestBase with MongoDbFixture with MemoryStreamFixture with BeforeAndAfter {
  import spark.implicits._
  import ScalaMongoImplicits._

  private val baseDirPath = TempDirectory("TestMongodbStreamWriter").deleteOnExit().path
  private val collectionName = "testcollection"

  behavior of "MongoDbStreamWriter"

  before {
    clearDb()
  }

  it should "write data to MongoDB" in {
    // given
    val inputData = Range(0, 100).toDF

    db.createCollection(collectionName)

    val config = new BaseConfiguration()
    config.addProperty(KEY_URI, s"$uri/$dbName.$collectionName")
    config.addProperty(StreamWriterCommonAttributes.keyCheckpointBaseLocation, s"$baseDirPath/checkpoint1")
    val writer = MongoDbStreamWriter(config).asInstanceOf[MongoDbStreamWriter]

    withStreamingData(inputData) { streamDf =>
      writeStream(writer, streamDf)

      assert(documentsInCollection == 100)
    }
  }

  it should "support checkpoints" in {
    // given
    val inputData1 = Range(0, 100).toDF
    val inputData2 = Range(0, 150).toDF

    db.createCollection(collectionName)

    val config = new BaseConfiguration()
    config.addProperty(KEY_URI, s"$uri/$dbName.$collectionName")
    config.addProperty(StreamWriterCommonAttributes.keyCheckpointBaseLocation, s"$baseDirPath/checkpoint2")
    val writer = MongoDbStreamWriter(config).asInstanceOf[MongoDbStreamWriter]

    withStreamingData(inputData1) { streamDf =>
      // Write first 100 records
      writeStream(writer, streamDf)

      assert(documentsInCollection == 100)
    }

    withStreamingData(inputData2) { streamDf =>
      writeStream(writer, streamDf)

      // Now write the updated stream that has 150 records.
      // It writes to the same collection. It should not have duplicates since a checkpoint
      // was created when the first 100 records were written.
      assert(documentsInCollection == 150)
    }

  }

  it should "write structured data to MongoDB" in {
    // given
    val inputData = Seq(
      ("John Doe", 48, 181.5, BigDecimal(10500.22), List(1, 10, 100)),
      ("Jane Williams", 39, 165.2, BigDecimal(1050011.22), List(200, 300)),
      ("N", 33, 171.4, BigDecimal("12345678901234567.5522"), List(1, 2, 3, 4, 5))
    ).toDF("name", "age", "height", "income", "numbers")

    db.createCollection(collectionName)

    val config = new BaseConfiguration()
    config.addProperty(KEY_URI, uri)
    config.addProperty(KEY_DATABASE, dbName)
    config.addProperty(KEY_COLLECTION, collectionName)
    config.addProperty(StreamWriterCommonAttributes.keyCheckpointBaseLocation, s"$baseDirPath/checkpoint3")
    val writer = MongoDbStreamWriter(config).asInstanceOf[MongoDbStreamWriter]

    // expected
    val expectedJson =
    """[
      |  {
      |    "name": "JaneWilliams",
      |    "age": 39,
      |    "height": 165.2,
      |    "income": 1050011.220000000000000000,
      |    "numbers": [ 200, 300 ]
      |  },
      |  {
      |    "name": "JohnDoe",
      |    "age": 48,
      |    "height": 181.5,
      |    "income": 10500.220000000000000000,
      |    "numbers": [ 1, 10, 100 ]
      |  },
      |  {
      |    "name": "N",
      |    "age": 33,
      |    "height": 171.4,
      |    "income": 12345678901234567.552200000000000000,
      |    "numbers": [ 1, 2, 3, 4, 5 ]
      |  }
      |]""".stripMargin.replaceAll("\\s", "")

    withStreamingData(inputData) { streamDf =>
      writeStream(writer, streamDf)

      assert(documentsInCollection == 3)

      val json = spark.read
        .format("mongo")
        .option("spark.mongodb.input.uri", s"$uri/$dbName.$collectionName")
        .load()
        .select("name", "age", "height", "income", "numbers")
        .orderBy("name")
        .toJSON
        .collect()
        .mkString("[", ",", "]")
        .replaceAll("\\s", "")

      assert(json == expectedJson)
    }
  }

  private def writeStream(writer: StreamWriter, streamDf: DataFrame): Unit = {
    val sink = writer.write(streamDf)
    sink.processAllAvailable()
    sink.stop()
  }

  private def documentsInCollection: Long = {
    db.getCollection(collectionName)
      .countDocuments()
      .execute()
  }

}
