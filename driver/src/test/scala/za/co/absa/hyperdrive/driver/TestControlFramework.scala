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

package za.co.absa.hyperdrive.driver

import org.apache.logging.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.streaming.{MemoryStream, StreamingQueryWrapper}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, StreamingQueryListener, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import za.co.absa.commons.io.TempDirectory

class TestControlFramework extends FlatSpec with BeforeAndAfter with MockitoSugar with Matchers {

  private val logger = LogManager.getLogger

  private var baseDir: TempDirectory = _

    private def baseDirPath = baseDir.path.toUri.toString

  private def destinationDir = s"$baseDirPath/destination"

  private def checkpointDir = s"$baseDirPath/checkpoint"

  private val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName(s"Commons unit testing SchemaUtils")
    .config("spark.ui.enabled", "false")
    .config("spark.debug.maxToStringFields", 100)
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.sql.hive.convertMetastoreParquet", false)
    .config("fs.defaultFS", "file:/")
    .config("spark.sql.streaming.streamingQueryListeners", "za.co.absa.hyperdrive.driver.ControlFrameworkListener")
    .getOrCreate()

  behavior of "TestControlFramework"

  before {
    baseDir = TempDirectory("TestControlFramework").deleteOnExit()
  }

  after {
    baseDir.delete()
  }

  behavior of SparkIngestor.getClass.getName

  it should "not die" in {
    val options = Map(
      "checkpointLocation" -> checkpointDir,
      "path" -> destinationDir
    )
    val dataSource = DataSource(spark, "parquet", options = options)
    val parquetSink = dataSource.createSink(OutputMode.Append())

    val processingTimeIntervalMs = 500
    import spark.implicits._
    val memoryStream = MemoryStream[Int](42, spark.sqlContext)
    val df = memoryStream.toDF()
      // for demonstration purposes, filter half of the values
      .filter(col("value") % 2 === lit(0))

    val query = df.writeStream
      .options(options)
      .trigger(Trigger.ProcessingTime(processingTimeIntervalMs))
      .foreachBatch { (batchDf: DataFrame, batchId: Long) =>
        // Option 2: Invoke Atum in foreachBatch
        // Here, batchDf.isStreaming is false, so we could also invoke Atum directly in principle
        // Question: Why can't we just put the whole query inside the foreachBatch? State, don't want to work against the framework
        val df2 = batchDf.persist() // persist to avoid multiple executions of the query
        val c = df2.count()
        println(s"Writer Measurement: batchId = $batchId. Count = $c")
        parquetSink.addBatch(batchId, df2)
      }
      .start()

    memoryStream.addData(1 to 10)
    query.processAllAvailable()
    // TODO: How to hook into query to get execution for every microbatch? Listener doesn't deliver lastExecution
    printOutputRows(query)

    memoryStream.addData(101 to 250)
    query.processAllAvailable()
    printOutputRows(query)

    memoryStream.addData(251 to 500)
    query.processAllAvailable()
    printOutputRows(query)

    query.stop()

    val c = spark.read.parquet(destinationDir).count()
    println(s"Total number of rows written: $c")
  }

  private def printOutputRows(query: StreamingQuery) = {
    // Option 3: Leverage execution plan to get number of output rows for each operation (not only read)
    // Inspiration: ProgressReporter.extractSourceToNumInputRows
    query.asInstanceOf[StreamingQueryWrapper].streamingQuery.lastExecution.executedPlan.collect {
      case p if p.metrics.contains("numOutputRows") =>
        println(s"${p.simpleString}. NumOutputRows: ${p.metrics("numOutputRows").value}")
    }
  }
}

class ControlFrameworkListener(sparkConf: SparkConf) extends StreamingQueryListener {
  override def onQueryStarted(event: QueryStartedEvent): Unit = {}

  override def onQueryProgress(event: QueryProgressEvent): Unit = {
    // Option 1: Use QueryProgressEvent. Only gets number of input rows. Uses numOutputRows of execution plan internally
    println(s"Reader Measurement: batchId = ${event.progress.batchId}. Count = ${event.progress.numInputRows}")
  }

  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {}
}

/**
 * Using query listener / foreachBatch. Note: The reader measurements with count = 0 are an artifact of processAllAvailable
Reader Measurement: batchId = 0. Count = 0
Writer Measurement: batchId = 0. Count = 5
Reader Measurement: batchId = 0. Count = 10
Reader Measurement: batchId = 1. Count = 0
Writer Measurement: batchId = 1. Count = 75
Reader Measurement: batchId = 1. Count = 150
Reader Measurement: batchId = 2. Count = 0
Writer Measurement: batchId = 2. Count = 125
Reader Measurement: batchId = 2. Count = 250
Reader Measurement: batchId = 3. Count = 0

Using execution plan
Filter ((value#4 % 2) = 0). NumOutputRows: 5
ScanV2 MemoryStreamDataSource$[value#4]. NumOutputRows: 10
Filter ((value#41 % 2) = 0). NumOutputRows: 75
ScanV2 MemoryStreamDataSource$[value#41]. NumOutputRows: 150
Filter ((value#78 % 2) = 0). NumOutputRows: 125
ScanV2 MemoryStreamDataSource$[value#78]. NumOutputRows: 250
Total number of rows written: 205
 */
