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

package za.co.absa.hyperdrive.ingestor.implementation.utils

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.commons.io.TempDirectory
import za.co.absa.spark.commons.test.SparkTestBase

class TestMetadataLogUtil extends AnyFlatSpec with Matchers with SparkTestBase with BeforeAndAfter {

  import spark.implicits._

  var baseDir: TempDirectory = _
  var baseDirPath: String = _

  behavior of MetadataLogUtil.getClass.getName

  before {
    baseDir = TempDirectory("TestMetadataLogUtil").deleteOnExit()
    baseDirPath = baseDir.path.toAbsolutePath.toUri.toString
  }

  after {
    baseDir.delete()
  }

  "getParquetFilesNotListedInMetadataLog" should "return empty set if no partial writes have occurred" in {
    import org.apache.spark.sql.functions.lit

    val destinationPath = s"$baseDirPath/destination"
    val input = MemoryStream[Int](1, spark.sqlContext)
    input.addData(List.range(0, 100))
    val query = input.toDF()
      .withColumn("part1", lit("val1"))
      .withColumn("part2", lit("val2"))
      .writeStream
      .partitionBy("part1", "part2")
      .trigger(Trigger.Once)
      .option("checkpointLocation", s"$baseDirPath/checkpoint")
      .format(source = "parquet")
      .outputMode(OutputMode.Append())
      .start(destinationPath)
    query.awaitTermination()

    val diff = MetadataLogUtil.getParquetFilesNotListedInMetadataLog(spark, destinationPath)

    diff.isSuccess shouldBe true
    diff.get shouldBe empty
  }

  it should "return the empty set if the root folder doesn't exist" in {
    val diff = MetadataLogUtil.getParquetFilesNotListedInMetadataLog(spark, "/tmp/non-existing")

    diff.isSuccess shouldBe true
    diff.get shouldBe empty
  }

  it should "return the empty set if the root folder is empty" in {
    val dir = TempDirectory()
    val diff = MetadataLogUtil.getParquetFilesNotListedInMetadataLog(spark, dir.path.toAbsolutePath.toUri.toString)

    diff.isSuccess shouldBe true
    diff.get shouldBe empty
    dir.delete()
  }

  it should "return the filenames that are missing in the metadata log" in {
    val destinationPath = s"$baseDirPath/destination"
    val input = MemoryStream[Int](1, spark.sqlContext)
    input.addData(List.range(0, 100))
    val query = input.toDF()
      .writeStream
      .trigger(Trigger.Once)
      .option("checkpointLocation", s"$baseDirPath/checkpoint")
      .format(source = "parquet")
      .outputMode(OutputMode.Append())
      .start(destinationPath)
    query.awaitTermination()

    // simulate partial write
    (1000 to 1500).toDF()
      .write
      .mode(SaveMode.Append)
      .parquet(s"$destinationPath/partition1=value1")

    val diff = MetadataLogUtil.getParquetFilesNotListedInMetadataLog(spark, destinationPath)

    diff.isSuccess shouldBe true
    diff.get should not be empty
    diff.get.foreach(path => path should include("partition1=value1"))
  }
}


