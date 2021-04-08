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
// scala> :load observe-batch.scala

import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.sql.functions._

import java.util.UUID
import scala.collection.JavaConverters._

class SparkQueryExecutionListener extends QueryExecutionListener {

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    qe.observedMetrics.get("checkpoint1").foreach { row =>
      println(s"checkpoint1 rowCount: ${row.getAs[Long]("rowCount")}")
      println(s"checkpoint1 sum: ${row.getAs[Long]("sum")}")
      println(s"checkpoint1 sumAbs: ${row.getAs[Long]("sumAbs")}")
      println(s"checkpoint1 crc32: ${row.getAs[Long]("crc32")}")
    }
    qe.observedMetrics.get("checkpoint2").foreach { row =>
      println(s"checkpoint2 rowCount: ${row.getAs[Long]("rowCount")}")
      println(s"checkpoint2 sum: ${row.getAs[Long]("sum")}")
      println(s"checkpoint2 sumAbs: ${row.getAs[Long]("sumAbs")}")
      println(s"checkpoint2 crc32: ${row.getAs[Long]("crc32")}")
    }
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
}
val queryExecutionListener = new SparkQueryExecutionListener
spark.listenerManager.register(queryExecutionListener)

val uuid = UUID.randomUUID().toString

val df = (1 to 100).map(_ * -1).toDF.
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
  )
df.write.
  parquet(s"/tmp/out-batch/$uuid")


// df.explain
