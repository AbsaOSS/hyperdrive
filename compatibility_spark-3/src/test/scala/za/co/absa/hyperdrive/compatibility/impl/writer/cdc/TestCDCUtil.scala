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

package za.co.absa.hyperdrive.compatibility.impl.writer.cdc

import org.apache.spark.sql.{DataFrame, Row}
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.sql.Timestamp

class TestCDCUtil extends AnyFlatSpec with MockitoSugar with Matchers with CDCTestBase {

  import spark.implicits._

  "getDataFrameWithSortColumns" should "return dataframe with sort columns" in {
    val timestamp = Timestamp.valueOf("2022-11-02 14:15:39.11973")

    val testInput: DataFrame = Seq(
      CDCEvent("1", "value1", timestamp, "PT"),
      CDCEvent("2", "value2", timestamp, "UP"),
      CDCEvent("3", "value3", timestamp, "DL"),
      CDCEvent("4", "value4", timestamp, "XX")
    ).toDF

    val expected: Seq[Row] = Seq(
      Row("1", "value1", timestamp, "PT", 1, "value1"),
      Row("2", "value2", timestamp, "UP", 2, "value2"),
      Row("3", "value3", timestamp, "DL", 3, "value3"),
      Row("4", "value4", timestamp, "XX", 0, "value4")
    )

    val sortFieldsPrefix = "_tmp_hyperdrive_"
    val precombineColumns = Seq(
      "eventType",
      "value"
    )
    val precombineColumnsCustomOrder = Map("eventType" -> Seq("PT", "UP", "DL"))

    val result = CDCUtil.getDataFrameWithSortColumns(
      testInput,
      sortFieldsPrefix,
      precombineColumns,
      precombineColumnsCustomOrder
    )

    result.collect().toSeq should contain theSameElementsAs expected
    result.schema.fieldNames should contain theSameElementsAs precombineColumns.map(col => s"$sortFieldsPrefix$col") ++ testInput.schema.fieldNames
  }

  "isDirEmptyOrDoesNotExist" should "return true if directory is empty" in {
    CDCUtil.isDirEmptyOrDoesNotExist(spark, baseDirUri) shouldBe true
  }

  it should "return true if directory does not exist" in {
    CDCUtil.isDirEmptyOrDoesNotExist(spark, destinationUri) shouldBe true
  }

  it should "return false if directory contains a file" in {
    new File(baseDirPath + "/filename.txt").createNewFile()

    CDCUtil.isDirEmptyOrDoesNotExist(spark, baseDirUri) shouldBe false
  }

  it should "return false if path is a file" in {
    val file = new File(baseDirPath + "/filename.txt")
    file.createNewFile()

    CDCUtil.isDirEmptyOrDoesNotExist(spark, file.toURI.getPath) shouldBe false
  }
}
