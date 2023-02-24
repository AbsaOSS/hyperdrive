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
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.commons.io.TempDirectory
import za.co.absa.spark.commons.test.SparkTestBase


class TestFileUtils extends AnyFlatSpec with Matchers with SparkTestBase with BeforeAndAfter {

  behavior of "FileUtils"
  private var baseDirectory: TempDirectory = _
  private var baseDirPath: String = _
  private val dummyDirectory = TempDirectory("DummyDirectory")
  private implicit val fs: FileSystem = FileSystem.get(dummyDirectory.path.toUri, spark.sparkContext.hadoopConfiguration)

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
    FileUtils.isEmpty(directory) shouldBe true
  }

  it should "return true if the directory only contains other directories, but no files" in {
    // given
    val directory = s"$baseDirPath/empty1"
    val subDirectory = s"$baseDirPath/empty1/empty2/empty3"
    fs.mkdirs(new Path(subDirectory))

    // when, then
    FileUtils.isEmpty(directory) shouldBe true
  }

  it should "return false if the directory is not empty" in {
    // given
    val directory = s"$baseDirPath/empty"
    val subDirectory = s"$baseDirPath/empty/empty2"
    fs.mkdirs(new Path(directory))
    fs.create(new Path(subDirectory, "_INFO"))

    // when, then
    FileUtils.isEmpty(directory) shouldBe false
  }

  it should "return false if the directory does not exist" in {
    // given
    val doesNotExist = s"$baseDirPath/${UUID.randomUUID().toString}"

    // when, then
    FileUtils.isEmpty(doesNotExist) shouldBe false
  }

  it should "return false if the argument is a file" in {
    // given
    val file = s"$baseDirPath/empty"
    fs.create(new Path(file))

    // when, then
    FileUtils.isEmpty(file) shouldBe false
  }
}
