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

  before {
    baseDirectory = TempDirectory("FileUtilsTest")
    baseDirectory.deleteOnExit()
  }

  after {
    baseDirectory.delete()
  }

  "isEmpty" should "return true if the directory is empty" in {
    // given
    val directory = s"$baseDirectory/empty"
    fs.mkdirs(new Path(directory))

    // when, then
    FileUtils.isEmpty(directory, config) shouldBe true
  }

  it should "return false if the directory is not empty" in {
    // given
    val directory = s"$baseDirectory/empty"
    fs.mkdirs(new Path(directory))
    fs.create(new Path(directory, "_INFO"))

    // when, then
    FileUtils.isEmpty(directory, config) shouldBe false
  }

  it should "return false if the directory does not exist" in {
    // given
    val doesNotExist = s"$baseDirectory/${UUID.randomUUID().toString}"

    // when, then
    FileUtils.isEmpty(doesNotExist, config) shouldBe false
  }

  it should "return false if the argument is a file" in {
    // given
    val file = s"$baseDirectory/empty"
    fs.create(new Path(file))

    // when, then
    FileUtils.isEmpty(file, config) shouldBe false
  }

}
