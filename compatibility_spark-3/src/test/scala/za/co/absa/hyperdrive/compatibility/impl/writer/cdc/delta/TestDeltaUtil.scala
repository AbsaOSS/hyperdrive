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

package za.co.absa.hyperdrive.compatibility.impl.writer.cdc.delta

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import za.co.absa.hyperdrive.compatibility.impl.writer.cdc.{CDCTestBase, FileUtils}

import java.io.File

class TestDeltaUtil extends AnyFlatSpec with MockitoSugar with Matchers with CDCTestBase {
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
}
