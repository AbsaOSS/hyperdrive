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

// Usage: Start spark shell v3.0.0+
// scala> :load observe-streaming.scala

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryListener}
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

import java.util.UUID

class SparkQueryExecutionListener extends StreamingQueryListener {
  override def onQueryProgress(event: QueryProgressEvent): Unit = {
    println(s"BatchId: ${event.progress.batchId}")
    Option(event.progress.observedMetrics.get("checkpoint1")).foreach(row => {
      println(s"Checkpoint 1 rowCount: ${row.getAs[Long]("rowCount")}")
      println(s"Checkpoint 1 sum: ${row.getAs[Long]("sum")}")
      println(s"Checkpoint 1 sumAbs: ${row.getAs[Long]("sumAbs")}")
      println(s"Checkpoint 1 crc32: ${row.getAs[Long]("crc32")}")
    })
    Option(event.progress.observedMetrics.get("checkpoint2")).foreach(row => {
      println(s"Checkpoint 2 rowCount: ${row.getAs[Long]("rowCount")}")
      println(s"Checkpoint 2 sum: ${row.getAs[Long]("sum")}")
      println(s"Checkpoint 2 sumAbs: ${row.getAs[Long]("sumAbs")}")
      println(s"Checkpoint 2 crc32: ${row.getAs[Long]("crc32")}")    })
  }

  def onQueryStarted(event: QueryStartedEvent): Unit = {}
  def onQueryTerminated(event: QueryTerminatedEvent): Unit = {}
}

val queryExecutionListener = new SparkQueryExecutionListener
spark.streams.addListener(queryExecutionListener)

val uuid = UUID.randomUUID().toString

import spark.implicits._
val input = MemoryStream[Int](42, spark.sqlContext)
val df = input.toDF().
  withColumn("crc32value", crc32(col("value").cast("String"))).
  observe("checkpoint1",
    count(lit(1)).as("rowCount"),
    // countDistinct(col("value")).as("distinctCount"), // distinct aggregates are not allowed
    sum(col("value")).as("sum"),
    sum(abs(col("value"))).as("sumAbs"),
    sum(col("crc32value")).as("crc32")
  ).
  filter("value % 2 == 0").
  observe("checkpoint2",
    count(lit(1)).as("rowCount"),
    // countDistinct(col("value")).as("distinctCount"), // distinct aggregates are not allowed
    sum(col("value")).as("sum"),
    sum(abs(col("value"))).as("sumAbs"),
    sum(col("crc32value")).as("crc32")
  ).
  writeStream.
  format("parquet").
  outputMode(OutputMode.Append()).
  option("checkpointLocation", s"/tmp/out-streaming/$uuid/checkpoint-location")
val query = df.start(s"/tmp/out-streaming/$uuid/bla")
input.addData((1 to 100).map(_ * -1))
query.processAllAvailable()
input.addData((101 to 300).map(_ * -1))
query.processAllAvailable()
input.addData((301 to 600).map(_ * -1))
query.processAllAvailable()
query.stop()


//query.explain
