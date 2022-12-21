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

package za.co.absa.hyperdrive.compatibility.impl.writer.cdc

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import za.co.absa.hyperdrive.shared.utils.FileUtils

import java.net.URI

object CDCUtil {

  private val StartDateColumn = "_start_date"
  private val EndDateColumn = "_end_date"
  private val IsCurrentColumn = "_is_current"
  private val IsOldDataColumn = "_is_old_data"
  private val SortFieldPrefix = "_tmp_hyperdrive_"
  private val OldData = "_old_data"
  private val NewData = "_new_data"
  private val SortFieldCustomOrderColumn = "_tmp_hyperdrive_sort_field_custom_order_"

  private[hyperdrive] case class SCD2Fields(keyColumn: String,
                                            timestampColumn: String,
                                            operationColumn: String,
                                            operationDeleteValues: Seq[String],
                                            precombineColumns: Seq[String],
                                            precombineColumnsCustomOrder: Map[String, Seq[String]])

  private[hyperdrive] def getStagedDataForSCD2(history: DataFrame, input: DataFrame, scd2Fields: SCD2Fields): DataFrame = {
    val uniqueChangesForEachKeyAndTimestamp = removeDuplicates(input, scd2Fields)
    val previousEvents = getPreviousEvents(history, uniqueChangesForEachKeyAndTimestamp, scd2Fields)
    val nextEvents = getNextEvents(history, uniqueChangesForEachKeyAndTimestamp, scd2Fields)

    val union = previousEvents.union(nextEvents).distinct().union(
      uniqueChangesForEachKeyAndTimestamp
        .withColumn(StartDateColumn, col(scd2Fields.timestampColumn))
        .withColumn(EndDateColumn, lit(null))
        .withColumn(IsCurrentColumn, lit(false))
        .withColumn(IsOldDataColumn, lit(false))
        .selectExpr(
          Seq(StartDateColumn, EndDateColumn, IsCurrentColumn) ++
            uniqueChangesForEachKeyAndTimestamp.columns ++
            Seq(IsOldDataColumn): _*
        )
    )

    val uniqueEvents = removeDuplicates(union, scd2Fields)
    setSCD2Fields(uniqueEvents, scd2Fields).drop(IsOldDataColumn)
  }

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

  private[hyperdrive] def getSchemaWithSCD2Fields(input: DataFrame): StructType = {
    StructType(
      Seq(
        StructField(StartDateColumn, TimestampType, nullable = false),
        StructField(EndDateColumn, TimestampType, nullable = true),
        StructField(IsCurrentColumn, BooleanType, nullable = false)
      ).toArray ++ input.schema.fields
    )
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

  private def removeDuplicates(input: DataFrame, scd2Fields: SCD2Fields): DataFrame = {
    val dataFrameWithSortColumns = getDataFrameWithSortColumns(input, SortFieldPrefix, scd2Fields.precombineColumns, scd2Fields.precombineColumnsCustomOrder)
    val sortColumnsWithPrefix = dataFrameWithSortColumns.schema.fieldNames.filter(_.startsWith(SortFieldPrefix))
    val window = Window
      .partitionBy(s"${scd2Fields.keyColumn}", s"${scd2Fields.timestampColumn}")
      .orderBy(sortColumnsWithPrefix.map(col(_).desc): _*)
    dataFrameWithSortColumns
      .withColumn("rank", row_number().over(window))
      .where("rank == 1")
      .drop("rank")
      .drop(sortColumnsWithPrefix: _*)
  }

  private def getPreviousEvents(history: DataFrame, uniqueChangesForEachKeyAndTimestamp: DataFrame, scd2Fields: SCD2Fields): DataFrame = {
    history.as(OldData).join(
      uniqueChangesForEachKeyAndTimestamp.as(NewData),
      col(s"$NewData.${scd2Fields.keyColumn}").equalTo(col(s"$OldData.${scd2Fields.keyColumn}"))
        .and(col(s"$NewData.${scd2Fields.timestampColumn}").>=(col(s"$OldData.$StartDateColumn")))
        .and(col(s"$NewData.${scd2Fields.timestampColumn}").<=(col(s"$OldData.$EndDateColumn")))
        .or(
          col(s"$NewData.${scd2Fields.keyColumn}").equalTo(col(s"$OldData.${scd2Fields.keyColumn}"))
            .and(col(s"$NewData.${scd2Fields.timestampColumn}").>=(col(s"$OldData.$StartDateColumn")))
            .and(col(s"$OldData.$IsCurrentColumn").equalTo(true))
        )
    ).select(s"$OldData.*").withColumn(s"$IsOldDataColumn", lit(true))
  }

  private def getNextEvents(history: DataFrame, uniqueChangesForEachKeyAndTimestamp: DataFrame, scd2Fields: SCD2Fields): DataFrame = {
    val window = Window
      .partitionBy(col(s"$OldData.${scd2Fields.keyColumn}"), col(s"$NewData.${scd2Fields.timestampColumn}"))
      .orderBy(col(s"$OldData.$StartDateColumn").asc, col(s"$OldData.${scd2Fields.timestampColumn}").asc)

    history.as(OldData).join(
      uniqueChangesForEachKeyAndTimestamp.as(NewData),
      col(s"$NewData.${scd2Fields.keyColumn}").equalTo(col(s"$OldData.${scd2Fields.keyColumn}"))
        .and(col(s"$NewData.${scd2Fields.timestampColumn}").<(col(s"$OldData.$StartDateColumn")))
    ).select(s"$OldData.*", s"$NewData.${scd2Fields.timestampColumn}")
      .withColumn("rank", row_number().over(window))
      .where("rank == 1")
      .drop("rank")
      .select(s"$OldData.*")
      .withColumn(s"$IsOldDataColumn", lit(true))
  }

  private def setSCD2Fields(dataFrame: DataFrame, scd2Fields: SCD2Fields): DataFrame = {
    val idWindowDesc = org.apache.spark.sql.expressions.Window
      .partitionBy(scd2Fields.keyColumn)
      .orderBy(col(scd2Fields.timestampColumn).desc, col(IsOldDataColumn).desc)
    dataFrame
      .withColumn(
        EndDateColumn,
        when(
          col(IsOldDataColumn).equalTo(true).and(
            lag(scd2Fields.keyColumn, 1, null).over(idWindowDesc).isNull
          ),
          col(EndDateColumn)
        ).when(
          col(IsOldDataColumn).equalTo(true).and(
            lag(IsOldDataColumn, 1, false).over(idWindowDesc).equalTo(true)
          ),
          col(EndDateColumn)
        ).otherwise(
          lag(StartDateColumn, 1, null).over(idWindowDesc)
        )
      )
      .withColumn(
        EndDateColumn,
        when(col(scd2Fields.operationColumn).isInCollection(scd2Fields.operationDeleteValues), col(StartDateColumn))
          .when(!col(scd2Fields.operationColumn).isInCollection(scd2Fields.operationDeleteValues), col(EndDateColumn))
          .otherwise(null)
      )
      .withColumn(
        IsCurrentColumn,
        when(col(EndDateColumn).isNull, lit(true)).otherwise(lit(false))
      )
  }

}
