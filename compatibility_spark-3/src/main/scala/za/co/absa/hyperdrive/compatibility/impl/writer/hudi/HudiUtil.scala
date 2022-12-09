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

package za.co.absa.hyperdrive.compatibility.impl.writer.hudi

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.functions.{col, expr, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.hyperdrive.shared.utils.FileUtils

import java.net.URI

object HudiUtil {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val SortFieldCustomOrderColumn = "_tmp_hyperdrive_sort_field_custom_order_"

  private[hyperdrive] def getDataFrameWithSortColumns(dataFrame: DataFrame, sortFieldsPrefix: String, precombineColumns: Seq[String], precombineColumnsCustomOrder: Map[String, Seq[String]]): DataFrame = {
    precombineColumns.foldLeft(dataFrame) { (df, precombineColumn) =>
      val order = precombineColumnsCustomOrder.getOrElse(precombineColumn, Seq.empty[String])
      order match {
        case o if o.isEmpty =>
          df.withColumn(s"$sortFieldsPrefix$precombineColumn", col(precombineColumn))
        case o =>
          df
            .withColumn(SortFieldCustomOrderColumn, lit(o.toArray))
            .withColumn(
              s"$sortFieldsPrefix$precombineColumn",
              expr(s"""array_position($SortFieldCustomOrderColumn,$precombineColumn)""")
            ).drop(SortFieldCustomOrderColumn)
      }
    }
  }

  private[hyperdrive] def isDirEmptyOrDoesNotExist(spark: SparkSession, destination: String): Boolean = {
    implicit val fs: FileSystem = FileSystem.get(new URI(destination), spark.sparkContext.hadoopConfiguration)
    if (FileUtils.exists(destination)) {
      if (FileUtils.isDirectory(destination)) {
        FileUtils.isEmpty(destination)
      } else {
        false
      }
    } else {
      true
    }
  }
}
