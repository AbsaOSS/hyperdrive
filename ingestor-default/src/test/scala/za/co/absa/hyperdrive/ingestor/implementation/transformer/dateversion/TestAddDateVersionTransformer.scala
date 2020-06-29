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

package za.co.absa.hyperdrive.ingestor.implementation.transformer.dateversion

import java.time.LocalDate

import org.apache.commons.configuration2.BaseConfiguration
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import za.co.absa.commons.io.TempDirectory
import za.co.absa.commons.spark.SparkTestBase
import za.co.absa.hyperdrive.ingestor.api.writer.StreamWriterProperties

class TestAddDateVersionTransformer extends FlatSpec with SparkTestBase with Matchers with BeforeAndAfter {

  import spark.implicits._

  private var baseDir: TempDirectory = _

  private def baseDirPath = baseDir.path.toUri.toString

  private def destinationDir = s"$baseDirPath/destination"

  private def checkpointDir = s"$baseDirPath/checkpoint"

  private val random = scala.util.Random

  behavior of "AddDateVersionTransformer"

  before {
    baseDir = TempDirectory("testAddDateVersion").deleteOnExit()
  }

  after {
    baseDir.delete()
  }

  it should "write partitioned by date and version=1 where destination directory is empty" in {
    val config = new BaseConfiguration()
    config.addProperty(AddDateVersionTransformer.KeyDestinationDirectory, destinationDir)
    config.addProperty(AddDateVersionTransformer.KeyReportDate, "2020-02-29")
    val underTest = AddDateVersionTransformer(config)
    val df = getDummyReadStream().toDF()
    executeQuery(underTest.transform(df))
    dfShouldContainDateAndVersion(spark.read.parquet(destinationDir), LocalDate.of(2020, 2, 29), Seq(1))
  }

  it should "write to partition version=2 when version=1 already exists for the same date" in {
    // given
    val config = new BaseConfiguration()
    config.addProperty(AddDateVersionTransformer.KeyDestinationDirectory, destinationDir)
    config.addProperty(AddDateVersionTransformer.KeyReportDate, "2020-02-29")
    val underTest = AddDateVersionTransformer(config)

    // when
    val stream = getDummyReadStream()
    stream.addData(List.range(1000, 1100))
    executeQuery(underTest.transform(stream.toDF()))
    val df1 = spark.read.parquet(destinationDir)
    dfShouldContainDateAndVersion(df1, LocalDate.of(2020, 2, 29), Seq(1))

    stream.addData(List.range(2000, 2100))
    executeQuery(underTest.transform(stream.toDF()))
    val df2 = spark.read.parquet(destinationDir)
    dfShouldContainDateAndVersion(df2, LocalDate.of(2020, 2, 29), Seq(1, 2))
  }

  private def executeQuery(df: DataFrame) = {
    val query = df
      .writeStream
      .option(StreamWriterProperties.CheckpointLocation, checkpointDir)
      .partitionBy(AddDateVersionTransformer.ColumnDate, AddDateVersionTransformer.ColumnVersion)
      .outputMode(OutputMode.Append)
      .trigger(Trigger.Once)
      .start(destinationDir)
    query.awaitTermination()
  }

  private def dfShouldContainDateAndVersion(df: DataFrame, date: LocalDate, versions: Seq[Int]): Unit = {
    df.select(AddDateVersionTransformer.ColumnDate)
      .distinct()
      .as[java.sql.Date]
      .collect().toList should contain theSameElementsAs Seq(java.sql.Date.valueOf(date))
    df.select(AddDateVersionTransformer.ColumnVersion)
      .distinct()
      .as[Int]
      .collect().toList should contain theSameElementsAs versions
  }

  private def getDummyReadStream() = {
    val input = MemoryStream[Int](random.nextInt(), spark.sqlContext)
    input.addData(List.range(0, 100))
    input
  }
}
