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

package za.co.absa.hyperdrive.ingestor.implementation.transformer.column.renaming

import org.apache.commons.configuration2.BaseConfiguration
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import za.co.absa.commons.io.TempDirectory
import za.co.absa.commons.spark.SparkTestBase
import za.co.absa.hyperdrive.ingestor.api.writer.StreamWriterProperties
import za.co.absa.hyperdrive.ingestor.implementation.writer.parquet.ParquetStreamWriter

class TestColumnRenamingStreamTransformer extends FlatSpec with SparkTestBase with Matchers with BeforeAndAfter {
  import spark.implicits._

  private var baseDir: TempDirectory = _

  private def baseDirPath = baseDir.path.toUri.toString

  private def destinationDir = s"$baseDirPath/destination"

  private def checkpointDir = s"$baseDirPath/checkpoint"

  private val random = scala.util.Random

  behavior of "ColumnRenamingStreamTransformer"

  before {
    baseDir = TempDirectory("testColumnRenamingStreamTransformer").deleteOnExit()
  }

  after {
    baseDir.delete()
  }

  it should "rename input column" in {
    val config = new BaseConfiguration()
    config.addProperty(ParquetStreamWriter.KEY_DESTINATION_DIRECTORY, destinationDir)
    config.addProperty(ColumnRenamingStreamTransformer.KEY_COLUMNS_FROM, "value")
    config.addProperty(ColumnRenamingStreamTransformer.KEY_COLUMNS_TO, "v")
    val underTest = ColumnRenamingStreamTransformer(config)
    val df = getDummyReadStream().toDF()

    executeQuery(underTest.transform(df))

    val actualDf = spark.read.parquet(destinationDir)

    assert(df.schema.exists(f => f.name == "value"))
    assert(!df.schema.exists(f => f.name == "v"))
    assert(actualDf.schema.exists(f => f.name == "v"))
    assert(!actualDf.schema.exists(f => f.name == "value"))
  }


  private def executeQuery(df: DataFrame): Unit = {
    val query = df
      .writeStream
      .option(StreamWriterProperties.CheckpointLocation, checkpointDir)
      .outputMode(OutputMode.Append)
      .trigger(Trigger.Once)
      .start(destinationDir)
    query.awaitTermination()
  }

  private def getDummyReadStream(): MemoryStream[Int] = {
    val input = MemoryStream[Int](random.nextInt(), spark.sqlContext)
    input.addData(List.range(0, 100))
    input
  }
}
