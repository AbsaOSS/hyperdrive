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

package za.co.absa.hyperdrive.ingestor.implementation.testutils

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.scalatest.Suite
import za.co.absa.spark.commons.test.SparkTestBase

trait MemoryStreamFixture {

  this: Suite with SparkTestBase =>

  def withStreamingData(inputDf: DataFrame)(f: DataFrame => Unit): Unit = {
    val memoryStream = new MemoryStream[Row](1, spark.sqlContext)(RowEncoder(inputDf.schema))
    val streamingDf: DataFrame = memoryStream.toDF()

    inputDf.collect().foreach(e => {
      memoryStream.addData(e)
    })

    f(streamingDf)
  }

}
