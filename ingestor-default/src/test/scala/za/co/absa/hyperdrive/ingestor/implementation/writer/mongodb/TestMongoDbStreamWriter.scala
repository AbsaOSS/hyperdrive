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

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.spark.commons.test.SparkTestBase

class TestMongoDbStreamWriter extends AnyFlatSpec with MockitoSugar with Matchers with SparkTestBase {

  private val connectionString = s"mongodb://localhost:1234"
  private val dbName = "unit_test_database"
  private val checkpointDir = "/tmp/mongo-checkpoint"

  private val configuration = new Configuration()

  behavior of "MongoDbStreamWriter"

  it should "throw on blank destination" in {
    assertThrows[IllegalArgumentException](new MongoDbStreamWriter(Trigger.Once(), checkpointDir, "  ", None, None, Map()))
  }

  it should "set Trigger.Once" in {
    val dataStreamWriter = getDataStreamWriter

    invokeWriter(dataStreamWriter, Map())
    verify(dataStreamWriter).trigger(Trigger.Once)
  }

  it should "set Trigger.ProcessingTime" in {
    val dataStreamWriter = getDataStreamWriter

    invokeWriter(dataStreamWriter, Map(), Trigger.ProcessingTime(1L))
    verify(dataStreamWriter).trigger(Trigger.ProcessingTime(1L))
  }

  it should "set output mode as 'append'" in {
    val dataStreamWriter = getDataStreamWriter

    invokeWriter(dataStreamWriter, Map())
    verify(dataStreamWriter).outputMode(OutputMode.Append())
  }

  it should "start DataStreamWriter" in {
    val dataStreamWriter = getDataStreamWriter

    invokeWriter(dataStreamWriter, Map())
    verify(dataStreamWriter).start()
  }

  it should " include extra options in case they exist" in {
    val dataStreamWriter = getDataStreamWriter

    val extraConfs = Map("key.1" -> "value-1",
      "key.2" -> "value-2",
      "spark.mongodb.output.database" -> dbName,
      "spark.mongodb.output.collection" -> "testcollection")

    invokeWriter(dataStreamWriter, extraConfs)
    verify(dataStreamWriter).start()
    verify(dataStreamWriter).options(extraConfs)
  }

  private def invokeWriter(dataStreamWriter: DataStreamWriter[Row],
                           extraOptions: Map[String,String],
                           trigger: Trigger = Trigger.Once(),
                           collection: Option[String] = Some("testcollection"),
                           checkpointLocation: String = checkpointDir): Unit = {
    val dataFrame = getDataFrame(dataStreamWriter)
    val writer = new MongoDbStreamWriter(trigger, checkpointLocation, connectionString, Some(dbName), collection, extraOptions)
    writer.write(dataFrame)
  }

  private def getDataStreamWriter: DataStreamWriter[Row] =
    mock[DataStreamWriter[Row]](withSettings().defaultAnswer(RETURNS_SELF))


  private def getDataFrame(dataStreamWriter: DataStreamWriter[Row]): DataFrame = {
    val sparkContext = mock[SparkContext]
    when(sparkContext.hadoopConfiguration).thenReturn(configuration)
    val sparkSession = mock[SparkSession]
    when(sparkSession.sparkContext).thenReturn(sparkContext)

    val dataFrame = mock[DataFrame]
    when(dataFrame.writeStream).thenReturn(dataStreamWriter)
    when(dataFrame.sparkSession).thenReturn(sparkSession)
    dataFrame
  }
}
