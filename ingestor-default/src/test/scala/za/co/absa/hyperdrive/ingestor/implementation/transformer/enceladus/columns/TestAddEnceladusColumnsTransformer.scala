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

package za.co.absa.hyperdrive.ingestor.implementation.transformer.enceladus.columns

import java.time.LocalDate

import org.apache.commons.configuration2.BaseConfiguration
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.commons.io.TempDirectory
import za.co.absa.spark.commons.test.SparkTestBase
import za.co.absa.hyperdrive.ingestor.api.writer.StreamWriterProperties
import za.co.absa.hyperdrive.ingestor.implementation.writer.parquet.ParquetStreamWriter

class TestAddEnceladusColumnsTransformer extends AnyFlatSpec with SparkTestBase with Matchers with BeforeAndAfter {

  import spark.implicits._

  private var baseDir: TempDirectory = _

  private def baseDirPath = baseDir.path.toUri.toString

  private def destinationDir = s"$baseDirPath/destination"

  private def checkpointDir = s"$baseDirPath/checkpoint"

  private val random = scala.util.Random

  behavior of "AddEnceladusColumnsTransformer"

  before {
    baseDir = TempDirectory("testAddEnceladusColumnsVersion").deleteOnExit()
  }

  after {
    baseDir.delete()
  }

  it should "write partitioned by date and version=1" in {
    val config = new BaseConfiguration()
    config.addProperty(ParquetStreamWriter.KEY_DESTINATION_DIRECTORY, destinationDir)
    config.addProperty(AddEnceladusColumnsTransformer.KeyReportDate, "2020-02-29")
    val underTest = AddEnceladusColumnsTransformer(config)
    val df = getDummyReadStream().toDF()

    executeQuery(underTest.transform(df))

    val actualDf = spark.read.parquet(destinationDir)
    val date = LocalDate.of(2020, 2, 29)
    actualDf.select(AddEnceladusColumnsTransformer.InfoDateStringColumn)
      .distinct()
      .as[String]
      .collect().toList should contain theSameElementsAs Seq("2020-02-29")
    actualDf.select(AddEnceladusColumnsTransformer.InfoDateColumn)
      .distinct()
      .as[java.sql.Date]
      .collect().toList should contain theSameElementsAs Seq(java.sql.Date.valueOf(date))
    actualDf.select(AddEnceladusColumnsTransformer.InfoVersionColumn)
      .distinct()
      .as[Int]
      .collect().toList should contain theSameElementsAs Seq(1)
    actualDf.select(AddEnceladusColumnsTransformer.ErrorColumn)
      .distinct()
      .as[Seq[ErrorMessage]]
      .collect().toList.flatten shouldBe empty
  }


  private def executeQuery(df: DataFrame) = {
    val query = df
      .writeStream
      .option(StreamWriterProperties.CheckpointLocation, checkpointDir)
      .partitionBy(AddEnceladusColumnsTransformer.InfoDateColumn, AddEnceladusColumnsTransformer.InfoVersionColumn)
      .outputMode(OutputMode.Append)
      .trigger(Trigger.Once)
      .start(destinationDir)
    query.awaitTermination()
  }

  private def getDummyReadStream() = {
    val input = MemoryStream[Int](random.nextInt(), spark.sqlContext)
    input.addData(List.range(0, 100))
    input
  }
}
