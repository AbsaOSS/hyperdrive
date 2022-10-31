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

package za.co.absa.hyperdrive.compatibility.impl.writer.delta.snapshot

import org.apache.spark.sql.streaming.Trigger
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}
import za.co.absa.hyperdrive.compatibility.impl.writer.delta.{CDCEvent, DeltaTestBase}

class TestDeltaCDCToSnapshotWriter extends FlatSpec with MockitoSugar with Matchers with DeltaTestBase {
  behavior of "DeltaCDCToSnapshotWriter"

  it should "merge cdc events and create latest snapshot table" in {
    writeOneInput("/delta-cdc-to-snapshot/first-input.csv")
    getResult should contain theSameElementsAs CDCEvent.loadFromFile("/delta-cdc-to-snapshot/first-expected.csv")

    writeOneInput("/delta-cdc-to-snapshot/second-input.csv")
    getResult should contain theSameElementsAs CDCEvent.loadFromFile("/delta-cdc-to-snapshot/second-expected.csv")
  }

  def writeOneInput(testedInputPath: String): Unit = {
    val writer = createDeltaCDCToSnapshotWriter()

    memoryStream.addData(CDCEvent.loadFromFile(testedInputPath))
    writer.write(memoryStream.toDF()).processAllAvailable()
  }

  private def createDeltaCDCToSnapshotWriter(): DeltaCDCToSnapshotWriter = new DeltaCDCToSnapshotWriter(
      destination = destinationPath,
      trigger = Trigger.Once(),
      checkpointLocation = checkpointPath,
      partitionColumns = Seq.empty,
      keyColumn = "id",
      operationColumn = "eventType",
      operationDeleteValues = Seq("DL", "FD"),
      precombineColumns = Seq("timestamp","eventType"),
      precombineColumnsCustomOrder = Map("eventType" -> Seq("PT", "FI", "RR", "UB", "UP", "DL", "FD")),
      extraConfOptions = Map.empty[String, String]
  )
}
