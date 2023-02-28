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

package za.co.absa.hyperdrive.ingestor.implementation.writer.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import za.co.absa.commons.io.TempDirectory
import za.co.absa.spark.commons.test.SparkTestBase

class TestParquetStreamWriter extends AnyFlatSpec with MockitoSugar with Matchers with SparkTestBase {

  private val tempDir = TempDirectory().deleteOnExit()
  private val parquetDestination = tempDir.path.resolve("test-parquet")
  private val configuration = new Configuration()

  behavior of "ParquetStreamWriter"

  it should "throw on blank destination" in {
    assertThrows[IllegalArgumentException](new ParquetStreamWriter(destination = "  ", Trigger.Once(), "/tmp/checkpoint", None, false, Map()))
  }

  it should "set format as 'parquet'" in {
    val dataStreamWriter = getDataStreamWriter

    invokeWriter(dataStreamWriter, Map())
    verify(dataStreamWriter).format("parquet")
  }

  it should "set Trigger.Once" in {
    val dataStreamWriter = getDataStreamWriter

    invokeWriter(dataStreamWriter, Map())
    verify(dataStreamWriter).trigger(Trigger.Once)
  }

  it should "set Trigger.ProcessingTime" in {
    val dataStreamWriter = getDataStreamWriter

    invokeWriter(dataStreamWriter, Map(), Trigger.ProcessingTime(1L))
    verify(dataStreamWriter).trigger(Trigger.ProcessingTime(1L))
  }

  it should "set output mode as 'append'" in {
    val dataStreamWriter = getDataStreamWriter

    invokeWriter(dataStreamWriter, Map())
    verify(dataStreamWriter).outputMode(OutputMode.Append())
  }

  it should "start DataStreamWriter" in {
    val dataStreamWriter = getDataStreamWriter

    invokeWriter(dataStreamWriter, Map())
    verify(dataStreamWriter).start(parquetDestination.toAbsolutePath.toString)
  }

  it should " include extra options in case they exist" in {
    val dataStreamWriter = getDataStreamWriter

    val extraConfs = Map("key.1" -> "value-1", "key.2" -> "value-2")

    invokeWriter(dataStreamWriter, extraConfs)
    verify(dataStreamWriter).start(parquetDestination.toAbsolutePath.toString)

    verify(dataStreamWriter).options(extraConfs)
  }

  it should "partition by given column names" in {
    val dataStreamWriter = getDataStreamWriter

    invokeWriter(dataStreamWriter, Map(), partitionColumns = Some(Seq("column1", "column2")))
    verify(dataStreamWriter).partitionBy("column1", "column2")
  }

  it should "throw an exception if the metadata log is inconsistent" in {
    import spark.implicits._
    val baseDir = TempDirectory("TestParquetStreamWriter").deleteOnExit()
    val destinationPath = s"${baseDir.path.toAbsolutePath.toUri.toString}/destination"
    val input = MemoryStream[Int](1, spark.sqlContext)
    input.addData(List.range(0, 100))
    val df = input.toDF()

    // simulate partial write
    (1000 to 1500).toDF()
      .write
      .mode(SaveMode.Append)
      .parquet(s"$destinationPath/partition1=value1")

    val writer = new ParquetStreamWriter(destinationPath, Trigger.Once(), "/tmp/checkpoint", None, true, Map())
    val throwable = intercept[IllegalStateException](writer.write(df))

    throwable.getMessage should include("Inconsistent Metadata Log.")
  }

  private def invokeWriter(dataStreamWriter: DataStreamWriter[Row],
                           extraOptions: Map[String,String], trigger: Trigger = Trigger.Once(),
                           checkpointLocation: String = "/tmp/checkpoint-location",
                           partitionColumns: Option[Seq[String]] = None,
                           doMetadataCheck: Boolean = false): Unit = {
    val dataFrame = getDataFrame(dataStreamWriter)
    val writer = new ParquetStreamWriter(parquetDestination.toAbsolutePath.toString, trigger, checkpointLocation,
      partitionColumns, doMetadataCheck, extraOptions)
    writer.write(dataFrame)
  }

  private def getDataStreamWriter: DataStreamWriter[Row] =
    mock[DataStreamWriter[Row]](withSettings().defaultAnswer(RETURNS_SELF))


  private def getDataFrame(dataStreamWriter: DataStreamWriter[Row]): DataFrame = {
    val sparkContext = mock[SparkContext]
    when(sparkContext.hadoopConfiguration).thenReturn(configuration)
    val sparkSession = mock[SparkSession]
    when(sparkSession.sparkContext).thenReturn(sparkContext)

    val dataFrame = mock[DataFrame]
    when(dataFrame.writeStream).thenReturn(dataStreamWriter)
    when(dataFrame.sparkSession).thenReturn(sparkSession)
    dataFrame
  }
}
