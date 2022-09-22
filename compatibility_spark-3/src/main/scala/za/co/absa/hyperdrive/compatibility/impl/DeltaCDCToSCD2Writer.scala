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

package za.co.absa.hyperdrive.compatibility.impl

import za.co.absa.hyperdrive.compatibility.api.{CompatibleDeltaCDCToSCD2Writer, DeltaCDCToSCD2WriterConfiguration}
import io.delta.tables.{DeltaMergeBuilder, DeltaTable}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Column, DataFrame, Row, SaveMode, SparkSession, functions}
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.functions.{col, lag, lit, row_number}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.slf4j.LoggerFactory
import org.apache.spark.sql.types.{BooleanType, TimestampType}

import java.net.URI

class DeltaCDCToSCD2Writer(configuration: DeltaCDCToSCD2WriterConfiguration) extends CompatibleDeltaCDCToSCD2Writer {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val CHECKPOINT_LOCATION = "checkpointLocation"

  private val STRING_SEPARATOR = "#$@"
  private val START_DATE_COLUMN = "_start_date"
  private val END_DATE_COLUMN = "_end_date"
  private val IS_CURRENT_COLUMN = "_is_current"
  private val MERGE_KEY_COLUMN = "_mergeKey"
  private val SORT_FIELD_PREFIX = "_tmp_hyperdrive_"

  if(configuration.precombineColumnsCustomOrder.values.flatten.toSeq.contains(STRING_SEPARATOR)) {
    throw new IllegalArgumentException(s"Precombine columns custom order cannot contain string separator: $STRING_SEPARATOR")
  }

  override def write(dataFrame: DataFrame): StreamingQuery = {
    dataFrame.writeStream
      .trigger(configuration.trigger)
      .outputMode(OutputMode.Append())
      .option(CHECKPOINT_LOCATION, configuration.checkpointLocation)
      .options(configuration.extraConfOptions)
      .foreachBatch((df: DataFrame, batchId: Long) => {
        if(!DeltaTable.isDeltaTable(df.sparkSession, configuration.destination)) {
          if(isDirEmptyOrDoesNotExist(df.sparkSession, configuration.destination)) {
            logger.info(s"Destination: ${configuration.destination} is not a delta table. Creating new delta table.")
            df.sparkSession
              .createDataFrame(
                df.sparkSession.sparkContext.emptyRDD[Row],
                df.schema
                  .add(START_DATE_COLUMN, TimestampType, false)
                  .add(END_DATE_COLUMN, TimestampType, true)
                  .add(IS_CURRENT_COLUMN, BooleanType, false)
              )
              .write
              .format("delta")
              .mode(SaveMode.Overwrite)
              .option("overwriteSchema", "true")
              .partitionBy(configuration.partitionColumns :_*)
              .save(configuration.destination)
          } else {
            throw new IllegalArgumentException(s"Could not create new delta table. Directory ${configuration.destination} is not empty!")
          }
        }
        logger.info(s"Writing batchId: $batchId")

        val dataFrameWithSortColumns = getDataFrameWithSortColumns(df, SORT_FIELD_PREFIX)

        val fieldNames = df.schema.fieldNames
        val originalFieldNames = df.schema.fieldNames.mkString(",")
        val sortColumnsWithPrefix = configuration.precombineColumns.map(precombineColumn => s"$SORT_FIELD_PREFIX$precombineColumn")

        val uniqueChangesForEachKeyAndTimestamp = dataFrameWithSortColumns
          .selectExpr(s"${configuration.keyColumn}", s"${configuration.timestampColumn}", s"struct(${sortColumnsWithPrefix.mkString(",")}, $originalFieldNames) as otherCols" )
          .groupBy(s"${configuration.keyColumn}", s"${configuration.timestampColumn}")
          .agg(functions.max("otherCols").as("latest"))
          .filter(col("latest").isNotNull)
          .withColumn("latest", new Column(AssertNotNull(col("latest").expr)))
          .selectExpr("latest.*")
          .drop(sortColumnsWithPrefix :_*)

        val stagedDataFrame = getStagedDataFrame(uniqueChangesForEachKeyAndTimestamp)


        generateDeltaMerge(stagedDataFrame, fieldNames).execute()
      })
      .start()
  }

  private def generateDeltaMerge(latestChanges: DataFrame, originalFieldNames: Seq[String]): DeltaMergeBuilder = {
    val initialDeltaBuilder = DeltaTable
      .forPath(configuration.destination)
      .as("currentTable")
      .merge(latestChanges.as("changes"), s"currentTable.$IS_CURRENT_COLUMN = true AND currentTable.${configuration.keyColumn} = changes.$MERGE_KEY_COLUMN")
      .whenMatched("currentTable.A_TIMSTAMP < changes.A_TIMSTAMP")
        .update(
          Map(
            s"$END_DATE_COLUMN" -> col("changes.A_TIMSTAMP"),
            s"$IS_CURRENT_COLUMN" -> lit(false)
          )
        )

    val deltaBuilderWithSortColumns = configuration.precombineColumns.foldLeft(initialDeltaBuilder) { (builder, precombineColumn) =>
          val order = configuration.precombineColumnsCustomOrder.getOrElse(precombineColumn, Seq.empty[String])
          order match {
            case o if o.isEmpty =>
              builder
                .whenMatched(s"currentTable.$precombineColumn < changes.$precombineColumn")
                .update(
                  Map(
                    s"$END_DATE_COLUMN" -> col("changes.A_TIMSTAMP"),
                    s"$IS_CURRENT_COLUMN" -> lit(false)
                  )
                )
            case o =>
              val orderString = o.mkString(STRING_SEPARATOR)
              builder
                .whenMatched(s"""locate(currentTable.$precombineColumn, "$orderString") < locate(changes.$precombineColumn, "$orderString")""")
                .update(
                  Map(
                    s"$END_DATE_COLUMN" -> col("changes.A_TIMSTAMP"),
                    s"$IS_CURRENT_COLUMN" -> lit(false)
                  )
                )
          }
        }
    deltaBuilderWithSortColumns
      .whenMatched()
      .delete()
      .whenNotMatched(s"changes.$MERGE_KEY_COLUMN is NOT NULL")
      .insert(
        originalFieldNames.map( c =>
          c -> col(s"changes.$c")
        ).toMap ++ Map(s"$START_DATE_COLUMN" -> col(s"changes.$START_DATE_COLUMN"),  s"$END_DATE_COLUMN" -> col(s"changes.$END_DATE_COLUMN"), s"$IS_CURRENT_COLUMN" -> col(s"changes.$IS_CURRENT_COLUMN"))
      )
  }

  private def getStagedDataFrame(dataFrame: DataFrame): DataFrame = {
    val idWindowDesc = org.apache.spark.sql.expressions.Window.partitionBy(configuration.keyColumn).orderBy(col(configuration.timestampColumn).desc)
    val insertRows = dataFrame
      .withColumn(s"$START_DATE_COLUMN", col(configuration.timestampColumn))
      .withColumn(s"$END_DATE_COLUMN", lag(s"$START_DATE_COLUMN", 1, null).over(idWindowDesc))
      .withColumn(s"$MERGE_KEY_COLUMN", lit(null))
      .withColumn(s"$END_DATE_COLUMN", functions.when(col(configuration.operationColumn).equalTo(configuration.operationDeleteValue), col(s"$START_DATE_COLUMN")).when(col(configuration.operationColumn).notEqual(configuration.operationDeleteValue), col(s"$END_DATE_COLUMN")).otherwise(null))
      .withColumn(s"$IS_CURRENT_COLUMN", functions.when(col(s"$END_DATE_COLUMN").isNull, lit(true)).otherwise(lit(false)))

    import dataFrame.sparkSession.implicits._
    val idWindowAsc = org.apache.spark.sql.expressions.Window.partitionBy(configuration.keyColumn).orderBy(col(s"$START_DATE_COLUMN").asc)
    val updateRows = insertRows.withColumn("_rn", row_number.over(idWindowAsc)).where($"_rn" === 1).drop("_rn").withColumn(s"$MERGE_KEY_COLUMN", col(configuration.keyColumn))

    insertRows.union(updateRows)
  }

  private def getDataFrameWithSortColumns(dataFrame: DataFrame, sortFieldsPrefix: String): DataFrame = {
    configuration.precombineColumns.foldLeft(dataFrame) { (df, precombineColumn) =>
      val order = configuration.precombineColumnsCustomOrder.getOrElse(precombineColumn, Seq.empty[String])
      order match {
        case o if o.isEmpty =>
          df.withColumn(s"$sortFieldsPrefix$precombineColumn", col(precombineColumn))
        case o =>
          val orderString = o.mkString(STRING_SEPARATOR)
          df.withColumn(s"$sortFieldsPrefix$precombineColumn", functions.expr(s"""locate($precombineColumn, "$orderString")"""))
      }
    }
  }

  private def isDirEmptyOrDoesNotExist(spark: SparkSession, destination: String): Boolean = {
    val fs: FileSystem = FileSystem.get(new URI(destination), spark.sparkContext.hadoopConfiguration)
    val path = new Path(destination)
    if(fs.exists(path)) {
      if(fs.isDirectory(path)) {
        !fs.listFiles(path, true).hasNext
      } else {
        false
      }
    } else {
      true
    }
  }
}

