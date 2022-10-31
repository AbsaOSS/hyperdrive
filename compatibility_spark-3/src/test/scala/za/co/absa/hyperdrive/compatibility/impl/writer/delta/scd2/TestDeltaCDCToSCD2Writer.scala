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

package za.co.absa.hyperdrive.compatibility.impl.writer.delta.scd2

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import za.co.absa.commons.io.TempDirectory
import za.co.absa.hyperdrive.compatibility.impl.writer.delta.{CDCEvent, DeltaEvent}
import za.co.absa.hyperdrive.shared.utils.SparkTestBase

import scala.reflect.io.Path

class TestDeltaCDCToSCD2Writer extends FlatSpec with MockitoSugar with Matchers with BeforeAndAfterEach with SparkTestBase {
  private val baseDir: TempDirectory = TempDirectory("TestDeltaCDCToSCD2Writer").deleteOnExit()
  private val destinationPath = s"${baseDir.path.toAbsolutePath.toString}/destination"
  private val checkpointPath = s"${baseDir.path.toAbsolutePath.toString}/checkpoint"

  import spark.implicits._

  private val memoryStream = MemoryStream[CDCEvent](1, spark.sqlContext)

  override def beforeEach(): Unit = {
    Path(destinationPath).deleteRecursively()
    Path(checkpointPath).deleteRecursively()
    memoryStream.reset()
  }

  behavior of "DeltaCDCToSCD2Writer"

  it should "merge unique by timestamp scd2 rows into empty delta table" in {
    writeOneInput("/01-empty-table-conflicting-dates/tested-input.csv")

    getResult should contain theSameElementsAs loadDeltaEvents("/01-empty-table-conflicting-dates/expected.csv")
  }

  it should "merge twice the same data" in {
    writeTwoInputs(
      "/02-twice-the-same-data/tested-input.csv",
      "/02-twice-the-same-data/tested-input.csv"
    )

    getResult should contain theSameElementsAs loadDeltaEvents("/02-twice-the-same-data/expected.csv")
  }

  it should "merge one row combinations" in {
    writeTwoInputs(
      "/03-merge-one-row-combinations/initial-input.csv",
      "/03-merge-one-row-combinations/tested-input.csv"
    )

    getResult should contain theSameElementsAs loadDeltaEvents("/03-merge-one-row-combinations/expected.csv")
  }

  it should "merge two rows combinations" in {
    writeTwoInputs(
      "/04-merge-two-rows-combinations/initial-input.csv",
      "/04-merge-two-rows-combinations/tested-input.csv"
    )

    getResult should contain theSameElementsAs loadDeltaEvents("/04-merge-two-rows-combinations/expected.csv")
  }

  it should "merge three rows combinations" in {
    writeTwoInputs(
      "/05-merge-three-rows-combinations/initial-input.csv",
      "/05-merge-three-rows-combinations/tested-input.csv"
    )
    getResult should contain theSameElementsAs loadDeltaEvents("/05-merge-three-rows-combinations/expected.csv")
  }

  it should "merge complex inputs" in {
    writeTwoInputs(
      "/06-complex-merges/initial-input.csv",
      "/06-complex-merges/tested-input.csv"
    )

    getResult should contain theSameElementsAs loadDeltaEvents("/06-complex-merges/expected.csv")
  }

  def writeOneInput(testedInputPath: String): Unit = {
    val writer = createDeltaCDCToSCD2Writer()

    memoryStream.addData(loadCDCEvents(testedInputPath))
    writer.write(memoryStream.toDF()).processAllAvailable()
  }

  def writeTwoInputs(initialInputPath: String, testedInputPath: String): Unit = {
    val writer = createDeltaCDCToSCD2Writer()

    memoryStream.addData(loadCDCEvents(initialInputPath))
    writer.write(memoryStream.toDF()).processAllAvailable()

    memoryStream.addData(loadCDCEvents(testedInputPath))
    writer.write(memoryStream.toDF()).processAllAvailable()
  }

  private def createDeltaCDCToSCD2Writer(): DeltaCDCToSCD2Writer = new DeltaCDCToSCD2Writer(
    destination = destinationPath,
    trigger = Trigger.Once(),
    checkpointLocation = checkpointPath,
    partitionColumns = Seq.empty,
    keyColumn = "id",
    timestampColumn = "timestamp",
    operationColumn = "eventType",
    operationDeleteValues = Seq("DL", "FD"),
    precombineColumns = Seq("eventType"),
    precombineColumnsCustomOrder = Map("eventType" -> Seq("PT", "FI", "RR", "UB", "UP", "DL", "FD")),
    extraConfOptions = Map.empty[String, String]
  )

  private def loadCDCEvents(path: String): Seq[CDCEvent] =
    CDCEvent.loadFromFile(getClass.getResource(s"/delta-cdc-to-scd2$path").getPath)

  private def loadDeltaEvents(path: String): Seq[DeltaEvent] =
    DeltaEvent.loadFromFile(getClass.getResource(s"/delta-cdc-to-scd2$path").getPath)

  private def getResult: Seq[DeltaEvent] = {
    import spark.implicits._
    spark.read.format("delta").load(destinationPath).as[DeltaEvent].collect().toSeq
  }
}
