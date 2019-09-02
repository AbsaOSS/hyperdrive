/*
 *  Copyright 2019 ABSA Group Limited
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package za.co.absa.hyperdrive.ingestor.implementation.writer.parquet

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import za.co.absa.hyperdrive.ingestor.api.manager.OffsetManager
import za.co.absa.hyperdrive.ingestor.api.writer.StreamWriter
import za.co.absa.hyperdrive.shared.test.utils.SparkTestUtils._

class TestAllNullableParquetStreamWriter extends FunSuite with BeforeAndAfterEach with MockitoSugar {

  private object dummyStreamWriter extends StreamWriter("/tmp/any") {
    override def getDestination: String = "/tmp/any"

    var capturedDataframe: DataFrame = _
    var capturedOffsetManager: OffsetManager = _

    override def write(dataFrame: DataFrame, offsetManager: OffsetManager): StreamingQuery = {
      capturedDataframe = dataFrame
      capturedOffsetManager = offsetManager
      null
    }
  }

  private val spark = SparkSession.builder().appName(classOf[TestAllNullableParquetStreamWriter].getSimpleName).master("local").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  private val stubOffsetManager = mock[OffsetManager]
  private val fakeDataFrame = getTestDataframe

  test(testName = "Dataframe has all columns nullabilities set to true before being passed to injected writer") {
    assert(!areAllFieldsNullable(fakeDataFrame.schema))

    val writer = new AllNullableParquetStreamWriter(dummyStreamWriter)
    writer.write(fakeDataFrame, stubOffsetManager)

    assert(dummyStreamWriter.capturedOffsetManager == stubOffsetManager)
    assert(areAllFieldsNullable(dummyStreamWriter.capturedDataframe.schema))
  }

  private def getTestDataframe: DataFrame = {
    val nestedType1 = new StructType()
      .add(StructField("a", IntegerType, nullable = false))
      .add(StructField("b", LongType, nullable = false))

    val nestedType2 = new StructType()
      .add(StructField("c", ArrayType(nestedType1, containsNull = false), nullable = false))

    val schema = new StructType()
      .add(StructField("d", StringType, nullable = false))
      .add(StructField("e", ArrayType(nestedType2, containsNull = false), nullable = false))

    spark.createDataFrame(spark.emptyDataFrame.rdd, schema)
  }
}
