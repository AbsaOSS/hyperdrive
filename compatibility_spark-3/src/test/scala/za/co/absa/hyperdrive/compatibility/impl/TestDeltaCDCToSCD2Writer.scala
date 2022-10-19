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

package za.co.absa.hyperdrive.compatibility.impl

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.scalatest.mockito.MockitoSugar
import za.co.absa.commons.io.TempDirectory
import za.co.absa.hyperdrive.compatibility.api.DeltaCDCToSCD2WriterConfiguration
import za.co.absa.hyperdrive.shared.utils.SparkTestBase

import java.sql.Timestamp
import scala.io.Source
import scala.util.Try

class TestDeltaCDCToSCD2Writer extends FlatSpec with MockitoSugar with Matchers with BeforeAndAfterEach with SparkTestBase {
  private val baseDir = TempDirectory("TestParquetStreamWriter").deleteOnExit()
  private val destinationPath = s"${baseDir.path.toAbsolutePath.toString}/destination"
  private val checkpointPath = s"${baseDir.path.toAbsolutePath.toString}/checkpoint"

  override def beforeEach(): Unit = {
    baseDir.deleteOnExit()
  }

  behavior of "DeltaCDCToSCD2Writer"

  it should "set format as 'parquet'" in {
    val inputData = CDCEvent.loadFromFile(getClass.getResource("/scd2Resources/mergeIntoEmptyTable/InputData.data").getPath)
    val expected = DeltaEvent.loadFromFile(getClass.getResource("/scd2Resources/mergeIntoEmptyTable/Expected.data").getPath)

    import spark.implicits._

        val input = MemoryStream[CDCEvent](1, spark.sqlContext)
    input.addData(inputData)


        val writer = createDeltaCDCToSCD2Writer()
        writer.write(input.toDF()).awaitTermination()

        val result = spark.read.format("delta").load(destinationPath).as[DeltaEvent].collect().toSeq

        result should contain theSameElementsAs expected
  }


  private def createDeltaCDCToSCD2Writer(): DeltaCDCToSCD2Writer = new DeltaCDCToSCD2Writer(
    DeltaCDCToSCD2WriterConfiguration(
      destination = destinationPath,
      trigger = Trigger.Once(),
      checkpointLocation = checkpointPath,
      partitionColumns = Seq.empty,
      keyColumn = "id",
      timestampColumn = "timestamp",
      operationColumn = "eventType",
      operationDeleteValue = "DL",
      precombineColumns = Seq("eventType"),
      precombineColumnsCustomOrder = Map("eventType" -> Seq("PT", "FI", "RR", "UB", "UP", "DL", "FD")),
      extraConfOptions = Map.empty[String, String]
    )
  )
}

case class CDCEvent(id: String, value: String, timestamp: Timestamp, eventType: String)
object CDCEvent {
  def loadFromFile(path: String): Seq[CDCEvent] = {
    val source = Source.fromFile(path)
    val events = for {
      line <- source.getLines().toVector
      values = line.split(",").map(_.trim)
    } yield CDCEvent(values(0), values(1), Timestamp.valueOf(values(2)), values(3))
    source.close()
    events
  }
}

case class DeltaEvent(id: String, value: String, timestamp: Timestamp, eventType: String, _start_date: Timestamp, _end_date: Timestamp, _is_current: Boolean)
object DeltaEvent {
  def loadFromFile(path: String): Seq[DeltaEvent] = {
    val source = Source.fromFile(path)
    val events = for {
      line <- source.getLines().toVector
      values = line.split(",").map(_.trim)
    } yield DeltaEvent(values(0), values(1), Timestamp.valueOf(values(2)), values(3), Timestamp.valueOf(values(4)), Try(Timestamp.valueOf(values(5))).getOrElse(null), values(6).toBoolean)
    source.close()
    events
  }
}


//trait SparkTestBase {
//  implicit val spark: SparkSession = SparkSession.builder()
//    .master("local[*]")
//    .appName(s"Commons unit testing SchemaUtils")
//    .config("spark.ui.enabled", "false")
//    .config("spark.debug.maxToStringFields", 100)
//    .config("spark.driver.bindAddress", "127.0.0.1")
//    .config("spark.driver.host", "127.0.0.1")
//    .config("spark.sql.hive.convertMetastoreParquet", false)
//    .config("fs.defaultFS", "file:/")
//    .getOrCreate()
//}
