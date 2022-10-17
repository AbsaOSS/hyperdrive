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
import org.apache.spark.sql.functions.{col, lag, lit}
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
  val OLD_DATA_PREFIX = "_oldData"
  val NEW_DATA_PREFIX = "_newData"

  if (configuration.precombineColumnsCustomOrder.values.flatten.toSeq.contains(STRING_SEPARATOR)) {
    throw new IllegalArgumentException(s"Precombine columns custom order cannot contain string separator: $STRING_SEPARATOR")
  }

  override def write(dataFrame: DataFrame): StreamingQuery = {
    dataFrame.writeStream
      .trigger(configuration.trigger)
      .outputMode(OutputMode.Append())
      .option(CHECKPOINT_LOCATION, configuration.checkpointLocation)
      .options(configuration.extraConfOptions)
      .foreachBatch((df: DataFrame, batchId: Long) => {
        if (!DeltaTable.isDeltaTable(df.sparkSession, configuration.destination)) {
          if (isDirEmptyOrDoesNotExist(df.sparkSession, configuration.destination)) {
            logger.info(s"Destination: ${configuration.destination} is not a delta table. Creating new delta table.")
            df.sparkSession
              .createDataFrame(
                df.sparkSession.sparkContext.emptyRDD[Row],
                df.schema
                  .add(START_DATE_COLUMN, TimestampType, nullable = false)
                  .add(END_DATE_COLUMN, TimestampType, nullable = true)
                  .add(IS_CURRENT_COLUMN, BooleanType, nullable = false)
              )
              .write
              .format("delta")
              .mode(SaveMode.Overwrite)
              .option("overwriteSchema", "true")
              .partitionBy(configuration.partitionColumns: _*)
              .save(configuration.destination)
          } else {
            throw new IllegalArgumentException(s"Could not create new delta table. Directory ${configuration.destination} is not empty!")
          }
        }
        logger.info(s"Writing batchId: $batchId")

        val deltaTable = DeltaTable.forPath(configuration.destination)

        val uniqueChangesForEachKeyAndTimestamp = removeDuplicates(df)
        val previousEvents = getPreviousEvents(deltaTable, uniqueChangesForEachKeyAndTimestamp)
        val nextEvents = getNextEvents(deltaTable, uniqueChangesForEachKeyAndTimestamp)

        val union = previousEvents.union(nextEvents).distinct().unionAll(
          uniqueChangesForEachKeyAndTimestamp
            .withColumn(START_DATE_COLUMN, col(configuration.timestampColumn))
            .withColumn(END_DATE_COLUMN, lit(null))
            .withColumn(IS_CURRENT_COLUMN, lit(false))
            .withColumn(MERGE_KEY_COLUMN, lit(null))
        )

        val stagedData = setSCD2Fields(union).drop(MERGE_KEY_COLUMN)
        val uniqueStagedData = removeDuplicates(stagedData)
        generateDeltaMerge(uniqueStagedData).execute()
      })
      .start()
  }

  private def getPreviousEvents(deltaTable: DeltaTable, uniqueChangesForEachKeyAndTimestamp: DataFrame): DataFrame = {
    deltaTable.toDF.as(OLD_DATA_PREFIX).join(
      uniqueChangesForEachKeyAndTimestamp.as(NEW_DATA_PREFIX),
      col(s"$NEW_DATA_PREFIX.${configuration.keyColumn}").equalTo(col(s"$OLD_DATA_PREFIX.${configuration.keyColumn}"))
        .and(col(s"$NEW_DATA_PREFIX.${configuration.timestampColumn}").>=(col(s"$OLD_DATA_PREFIX.$START_DATE_COLUMN")))
        .and(col(s"$NEW_DATA_PREFIX.${configuration.timestampColumn}").<=(col(s"$OLD_DATA_PREFIX.$END_DATE_COLUMN")))
        .or(
          col(s"$NEW_DATA_PREFIX.${configuration.keyColumn}").equalTo(col(s"$OLD_DATA_PREFIX.${configuration.keyColumn}"))
            .and(col(s"$NEW_DATA_PREFIX.${configuration.timestampColumn}").>=(col(s"$OLD_DATA_PREFIX.$START_DATE_COLUMN")))
            .and(col(s"$OLD_DATA_PREFIX.$IS_CURRENT_COLUMN").equalTo(true))
        )
    ).select(s"$OLD_DATA_PREFIX.*").withColumn(s"$MERGE_KEY_COLUMN", col(s"${configuration.keyColumn}"))
  }

  private def getNextEvents(deltaTable: DeltaTable, uniqueChangesForEachKeyAndTimestamp: DataFrame): DataFrame = {
    val fieldNames = deltaTable.toDF.schema.fieldNames
      .filter(_ != START_DATE_COLUMN)
      .filter(_ != configuration.timestampColumn)
    val originalFieldNames = deltaTable.toDF.schema.fieldNames
    deltaTable.toDF.as(OLD_DATA_PREFIX).join(
      uniqueChangesForEachKeyAndTimestamp.as(NEW_DATA_PREFIX),
      col(s"$NEW_DATA_PREFIX.${configuration.keyColumn}").equalTo(col(s"$OLD_DATA_PREFIX.${configuration.keyColumn}"))
        .and(col(s"$NEW_DATA_PREFIX.${configuration.timestampColumn}").<(col(s"$OLD_DATA_PREFIX.$START_DATE_COLUMN")))
    ).select(s"$OLD_DATA_PREFIX.*", s"$NEW_DATA_PREFIX.${configuration.timestampColumn}")
      .selectExpr(
        s"${configuration.keyColumn}",
        s"$NEW_DATA_PREFIX.${configuration.timestampColumn}",
        s"struct($START_DATE_COLUMN, $OLD_DATA_PREFIX.${configuration.timestampColumn}, ${fieldNames.mkString(",")}) as otherCols"
      )
      .groupBy(s"${configuration.keyColumn}", s"$NEW_DATA_PREFIX.${configuration.timestampColumn}")
      .agg(functions.min("otherCols").as("latest"))
      .filter(col("latest").isNotNull)
      .withColumn("latest", new Column(AssertNotNull(col("latest").expr)))
      .selectExpr("latest.*")
      .select(originalFieldNames.head, originalFieldNames.tail: _*)
      .withColumn(s"$MERGE_KEY_COLUMN", col(s"${configuration.keyColumn}"))
  }

  private def generateDeltaMerge(latestChanges: DataFrame): DeltaMergeBuilder = {
    DeltaTable
      .forPath(configuration.destination)
      .as("currentTable")
      .merge(
        latestChanges.as("changes"), s"currentTable.${configuration.keyColumn} = changes.${configuration.keyColumn} AND currentTable.$START_DATE_COLUMN = changes.$START_DATE_COLUMN"
      )
      .whenMatched()
      .updateAll()
      .whenNotMatched()
      .insertAll()
  }

  private def setSCD2Fields(dataFrame: DataFrame): DataFrame = {
    val idWindowDesc = org.apache.spark.sql.expressions.Window
      .partitionBy(configuration.keyColumn)
      .orderBy(col(configuration.timestampColumn).desc, col(MERGE_KEY_COLUMN).desc)
    dataFrame
      .withColumn(
        START_DATE_COLUMN,
        col(configuration.timestampColumn)
      )

      .withColumn(
        END_DATE_COLUMN,
        functions.when(
          col(MERGE_KEY_COLUMN).isNotNull.and(
            lag(MERGE_KEY_COLUMN, 1, null).over(idWindowDesc).isNotNull
          ),
          col(END_DATE_COLUMN)
        ).when(
          col(MERGE_KEY_COLUMN).isNotNull.and(
            lag(MERGE_KEY_COLUMN, 1, null).over(idWindowDesc).isNull
          ).and(
            col(configuration.timestampColumn).equalTo(
              lag(s"${configuration.timestampColumn}", 1, null).over(idWindowDesc)
            )
          ),
          lag(START_DATE_COLUMN, 2, null).over(idWindowDesc)
        ).otherwise(
          lag(START_DATE_COLUMN, 1, null).over(idWindowDesc)
        )
      )
      .withColumn(
        END_DATE_COLUMN,
        functions
          .when(col(configuration.operationColumn).equalTo(configuration.operationDeleteValue), col(START_DATE_COLUMN))
          .when(col(configuration.operationColumn).notEqual(configuration.operationDeleteValue), col(END_DATE_COLUMN))
          .otherwise(null)
      )
      .withColumn(
        IS_CURRENT_COLUMN,
        functions.when(col(END_DATE_COLUMN).isNull, lit(true)).otherwise(lit(false))
      )
  }

  private def removeDuplicates(inputDF: DataFrame): DataFrame = {
    val dataFrameWithSortColumns = getDataFrameWithSortColumns(inputDF, SORT_FIELD_PREFIX)

    val originalFieldNames = inputDF.schema.fieldNames.mkString(",")
    val sortColumnsWithPrefix = configuration.precombineColumns.map(precombineColumn =>
      s"$SORT_FIELD_PREFIX$precombineColumn"
    )

    dataFrameWithSortColumns
      .selectExpr(
        s"${configuration.keyColumn}",
        s"${configuration.timestampColumn}",
        s"struct(${sortColumnsWithPrefix.mkString(",")}, $originalFieldNames) as otherCols"
      )
      .groupBy(s"${configuration.keyColumn}", s"${configuration.timestampColumn}")
      .agg(functions.max("otherCols").as("latest"))
      .filter(col("latest").isNotNull)
      .withColumn("latest", new Column(AssertNotNull(col("latest").expr)))
      .selectExpr("latest.*")
      .drop(sortColumnsWithPrefix: _*)
  }

  private def getDataFrameWithSortColumns(dataFrame: DataFrame, sortFieldsPrefix: String): DataFrame = {
    configuration.precombineColumns.foldLeft(dataFrame) { (df, precombineColumn) =>
      val order = configuration.precombineColumnsCustomOrder.getOrElse(precombineColumn, Seq.empty[String])
      order match {
        case o if o.isEmpty =>
          df.withColumn(s"$sortFieldsPrefix$precombineColumn", col(precombineColumn))
        case o =>
          val orderString = o.mkString(STRING_SEPARATOR)
          df.withColumn(
            s"$sortFieldsPrefix$precombineColumn",
            functions.expr(s"""locate($precombineColumn, "$orderString")""")
          )
      }
    }
  }

  private def isDirEmptyOrDoesNotExist(spark: SparkSession, destination: String): Boolean = {
    val fs: FileSystem = FileSystem.get(new URI(destination), spark.sparkContext.hadoopConfiguration)
    val path = new Path(destination)
    if (fs.exists(path)) {
      if (fs.isDirectory(path)) {
        !fs.listFiles(path, true).hasNext
      } else {
        false
      }
    } else {
      true
    }
  }
}

