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

package za.co.absa.hyperdrive.compatibility.impl.writer.cdc.hudi.scd2

import org.apache.spark.sql.streaming.Trigger
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.hyperdrive.compatibility.impl.writer.cdc.{CDCEvent, CDCTestBase, SCD2Event}

class TestHudiCDCToSCD2Writer extends AnyFlatSpec with MockitoSugar with Matchers with CDCTestBase {
  behavior of "HudiCDCToSCD2Writer"

  it should "merge unique by timestamp scd2 rows into empty hudi table" in {
    writeOneInput("/cdc-to-scd2/01-empty-table-conflicting-dates/tested-input.csv")

    getResult should contain theSameElementsAs SCD2Event.loadFromFile("/cdc-to-scd2/01-empty-table-conflicting-dates/expected.csv")
  }

  it should "merge twice the same data" in {
    writeTwoInputs(
      "/cdc-to-scd2/02-twice-the-same-data/tested-input.csv",
      "/cdc-to-scd2/02-twice-the-same-data/tested-input.csv"
    )

    getResult should contain theSameElementsAs SCD2Event.loadFromFile("/cdc-to-scd2/02-twice-the-same-data/expected.csv")
  }

  it should "merge one row combinations" in {
    writeTwoInputs(
      "/cdc-to-scd2/03-merge-one-row-combinations/initial-input.csv",
      "/cdc-to-scd2/03-merge-one-row-combinations/tested-input.csv"
    )

    getResult should contain theSameElementsAs SCD2Event.loadFromFile("/cdc-to-scd2/03-merge-one-row-combinations/expected.csv")
  }

  it should "merge two rows combinations" in {
    writeTwoInputs(
      "/cdc-to-scd2/04-merge-two-rows-combinations/initial-input.csv",
      "/cdc-to-scd2/04-merge-two-rows-combinations/tested-input.csv"
    )

    getResult should contain theSameElementsAs SCD2Event.loadFromFile("/cdc-to-scd2/04-merge-two-rows-combinations/expected.csv")
  }

  it should "merge three rows combinations" in {
    writeTwoInputs(
      "/cdc-to-scd2/05-merge-three-rows-combinations/initial-input.csv",
      "/cdc-to-scd2/05-merge-three-rows-combinations/tested-input.csv"
    )
    getResult should contain theSameElementsAs SCD2Event.loadFromFile("/cdc-to-scd2/05-merge-three-rows-combinations/expected.csv")
  }

  it should "merge complex inputs" in {
    writeTwoInputs(
      "/cdc-to-scd2/06-complex-merges/initial-input.csv",
      "/cdc-to-scd2/06-complex-merges/tested-input.csv"
    )

    getResult should contain theSameElementsAs SCD2Event.loadFromFile("/cdc-to-scd2/06-complex-merges/expected.csv")
  }

  def writeOneInput(testedInputPath: String): Unit = {
    val writer = createHudiCDCToSCD2Writer()

    memoryStream.addData(CDCEvent.loadFromFile(testedInputPath))
    writer.write(memoryStream.toDF()).processAllAvailable()
  }

  def writeTwoInputs(initialInputPath: String, testedInputPath: String): Unit = {
    val writer = createHudiCDCToSCD2Writer()

    memoryStream.addData(CDCEvent.loadFromFile(initialInputPath))
    writer.write(memoryStream.toDF()).processAllAvailable()

    memoryStream.addData(CDCEvent.loadFromFile(testedInputPath))
    writer.write(memoryStream.toDF()).processAllAvailable()
  }

  private def createHudiCDCToSCD2Writer(): HudiCDCToSCD2Writer = new HudiCDCToSCD2Writer(
    destination = destinationUri,
    trigger = Trigger.Once(),
    checkpointLocation = checkpointUri,
    partitionColumns = Seq.empty,
    keyColumn = "id",
    timestampColumn = "timestamp",
    operationColumn = "eventType",
    operationDeleteValues = Seq("DL", "FD"),
    precombineColumns = Seq("eventType"),
    precombineColumnsCustomOrder = Map("eventType" -> Seq("PT", "FI", "RR", "UB", "UP", "DL", "FD")),
    extraConfOptions = Map.empty[String, String]
  )

  private def getResult: Seq[SCD2Event] = {
    import spark.implicits._
    spark.read.format("hudi").load(destinationPath).as[SCD2Event].collect().toSeq
  }
}
