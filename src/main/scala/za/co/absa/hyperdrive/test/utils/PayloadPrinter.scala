/*
 * Copyright 2018 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.hyperdrive.test.utils

import org.apache.spark.sql.SparkSession

object PayloadPrinter {

  val FORMAT_PARQUET = "parquet"
  val FORMAT_JSON = "json"

  def main(args: Array[String]): Unit = {
    showContent("/tmp/HYPERDRIVE_PAYLOAD/dest2", FORMAT_JSON)
  }

  def showContent(sourceDir: String, format: String): Unit = {
    println(s"Going to print Parquet content from '$sourceDir'")
    val spark = SparkSession.builder().appName("ParquetPresenter").master("local[*]").getOrCreate()

    val data = format match {
      case FORMAT_JSON => spark.read.json(sourceDir)
      case FORMAT_PARQUET => spark.read.parquet(sourceDir)
    }

    println(s"TOTAL messages in '$sourceDir': ${data.count()}")
    data.show(false)
  }
}
