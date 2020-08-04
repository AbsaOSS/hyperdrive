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

package za.co.absa.hyperdrive.shared.utils

import java.util.UUID

import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import za.co.absa.commons.io.TempDirectory
import za.co.absa.commons.spark.SparkTestBase

class TestFileUtils extends FlatSpec with Matchers with SparkTestBase with BeforeAndAfter {

  behavior of "FileUtils"
  private val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  private val config = spark.sparkContext.hadoopConfiguration
  private var baseDirectory: TempDirectory = _
  private var baseDirPath: String = _

  before {
    baseDirectory = TempDirectory("FileUtilsTest")
    baseDirectory.deleteOnExit()
    baseDirPath = baseDirectory.path.toAbsolutePath.toString
  }

  after {
    baseDirectory.delete()
  }

  "isEmpty" should "return true if the directory is empty" in {
    // given
    val directory = s"$baseDirPath/empty"
    fs.mkdirs(new Path(directory))

    // when, then
    FileUtils.isEmpty(directory, config) shouldBe true
  }

  it should "return false if the directory is not empty" in {
    // given
    val directory = s"$baseDirPath/empty"
    fs.mkdirs(new Path(directory))
    fs.create(new Path(directory, "_INFO"))

    // when, then
    FileUtils.isEmpty(directory, config) shouldBe false
  }

  it should "return false if the directory does not exist" in {
    // given
    val doesNotExist = s"$baseDirPath/${UUID.randomUUID().toString}"

    // when, then
    FileUtils.isEmpty(doesNotExist, config) shouldBe false
  }

  it should "return false if the argument is a file" in {
    // given
    val file = s"$baseDirPath/empty"
    fs.create(new Path(file))

    // when, then
    FileUtils.isEmpty(file, config) shouldBe false
  }

  "dirContainsNoParquetFilesOrDoesNotExist" should "return true if directory does not exist" in {
    // given
    val dir = s"$baseDirPath/somedir"

    // when, then
    FileUtils.dirContainsNoParquetFilesOrDoesNotExist(dir, config) shouldBe true
  }

  it should "return true if directory and subdirs don't contain parquet files" in {
    // given
    val subDir = s"$baseDirPath/somepartition=somevalue/foo=bar"
    val parquetInHiddenDir1 = s"$baseDirPath/_temporary/0.parquet"
    val parquetInHiddenDir2 = s"$baseDirPath/.hidden/0.parquet"

    fs.create(new Path(subDir))
    fs.create(new Path(parquetInHiddenDir1))
    fs.create(new Path(parquetInHiddenDir2))

    // when, then
    FileUtils.dirContainsNoParquetFilesOrDoesNotExist(baseDirPath, config) shouldBe true
  }

  it should "return false if directory contains parquet files" in {
    // given
    val parquetFile = s"$baseDirPath/0.parquet"
    fs.create(new Path(parquetFile))

    // when, then
    FileUtils.dirContainsNoParquetFilesOrDoesNotExist(baseDirPath, config) shouldBe false
  }

  it should "return false if subdir contains parquet files" in {
    // given
    val parquetInSubDir = s"$baseDirPath/somepartition=somevalue/foo=bar/0.parquet"
    val someOtherSubDir = s"$baseDirPath/somepartition=somevalue/foo2=bar2"
    fs.create(new Path(parquetInSubDir))
    fs.create(new Path(someOtherSubDir))

    // when, then
    FileUtils.dirContainsNoParquetFilesOrDoesNotExist(baseDirPath, config) shouldBe false
  }

}
