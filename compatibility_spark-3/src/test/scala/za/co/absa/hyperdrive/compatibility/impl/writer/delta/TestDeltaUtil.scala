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

package za.co.absa.hyperdrive.compatibility.impl.writer.delta

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.mockito.MockitoSugar

import java.io.File
import java.sql.Timestamp

class TestDeltaUtil extends FlatSpec with MockitoSugar with Matchers with DeltaTestBase {
  behavior of "DeltaUtil"

  "createDeltaTableIfNotExists" should "create delta table if destination directory is empty" in {
    val schema = StructType(Seq(StructField("testColumn", StringType, nullable = false)))

    DeltaUtil.createDeltaTableIfNotExists(spark, baseDirUri, schema, Seq())

    FileUtils.readFileLines(s"$baseDirPath/_delta_log/00000000000000000000.json").nonEmpty shouldBe true
  }

  it should "create delta table if destination directory does not exist" in {
    val schema = StructType(Seq(StructField("testColumn", StringType, nullable = false)))

    DeltaUtil.createDeltaTableIfNotExists(spark, destinationUri, schema, Seq())

    FileUtils.readFileLines(s"$destinationPath/_delta_log/00000000000000000000.json").nonEmpty shouldBe true
  }

  it should "do nothing if delta table already exists" in {
    val schema = StructType(Seq(StructField("testColumn", StringType, nullable = false)))
    val dataFolder = new File(destinationPath)
    val deltaLogFolder = new File(s"$destinationPath/_delta_log/")

    DeltaUtil.createDeltaTableIfNotExists(spark, dataFolder.toURI.getPath, schema, Seq())
    FileUtils.readFileLines(s"${deltaLogFolder.getPath}/00000000000000000000.json").nonEmpty shouldBe true
    val contentOfDataFolderFirstExec = dataFolder.list()
    val contentOfDeltaLogFolderFirstExec = deltaLogFolder.list()

    DeltaUtil.createDeltaTableIfNotExists(spark, dataFolder.toURI.getPath, schema, Seq())
    FileUtils.readFileLines(s"${deltaLogFolder.getPath}/00000000000000000000.json").nonEmpty shouldBe true
    val contentOfDataFolderSecondExec = dataFolder.list()
    val contentOfDeltaLogFolderSecondExec = deltaLogFolder.list()

    contentOfDataFolderFirstExec should contain theSameElementsAs contentOfDataFolderSecondExec
    contentOfDeltaLogFolderFirstExec should contain theSameElementsAs contentOfDeltaLogFolderSecondExec
  }

  it should "throw exception if destination directory is not empty" in {
    val schema = StructType(Seq(StructField("testColumn", StringType, nullable = false)))

    new File(baseDirPath + "/filename.txt").createNewFile()

    assertThrows[IllegalArgumentException](DeltaUtil.createDeltaTableIfNotExists(spark, baseDirUri, schema, Seq()))
  }

  "getDataFrameWithSortColumns" should "return dataframe with sort columns" in {
    import spark.implicits._
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

    val result = DeltaUtil.getDataFrameWithSortColumns(
      testInput,
      sortFieldsPrefix,
      precombineColumns,
      precombineColumnsCustomOrder
    )

    result.collect().toSeq should contain theSameElementsAs expected
    result.schema.fieldNames should contain theSameElementsAs precombineColumns.map(col => s"$sortFieldsPrefix$col") ++ testInput.schema.fieldNames
  }

  "isDirEmptyOrDoesNotExist" should "return true if directory is empty" in {
    DeltaUtil.isDirEmptyOrDoesNotExist(spark, baseDirUri) shouldBe true
  }

  it should "return true if directory does not exist" in {
    DeltaUtil.isDirEmptyOrDoesNotExist(spark, destinationUri) shouldBe true
  }

  it should "return false if directory contains a file" in {
    new File(baseDirPath + "/filename.txt").createNewFile()

    DeltaUtil.isDirEmptyOrDoesNotExist(spark, baseDirUri) shouldBe false
  }

  it should "return false if path is a file" in {
    val file = new File(baseDirPath + "/filename.txt")
    file.createNewFile()

    DeltaUtil.isDirEmptyOrDoesNotExist(spark, file.toURI.getPath) shouldBe false
  }
}
