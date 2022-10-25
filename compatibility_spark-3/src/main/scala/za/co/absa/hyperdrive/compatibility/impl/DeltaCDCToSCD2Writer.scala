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

  private val CheckpointLocation = "checkpointLocation"

  private val StringSeparator = "#$@"
  private val StartDateColumn = "_start_date"
  private val EndDateColumn = "_end_date"
  private val IsCurrentColumn = "_is_current"
  private val MergeKeyColumn = "_mergeKey"
  private val SortFieldPrefix = "_tmp_hyperdrive_"
  private val OldDataPrefix = "_oldData"
  private val NewDataPrefix = "_newData"

  if (configuration.precombineColumnsCustomOrder.values.flatten.toSeq.contains(StringSeparator)) {
    throw new IllegalArgumentException(s"Precombine columns custom order cannot contain string separator: $StringSeparator")
  }

  override def write(dataFrame: DataFrame): StreamingQuery = {
    dataFrame.writeStream
      .trigger(configuration.trigger)
      .outputMode(OutputMode.Append())
      .option(CheckpointLocation, configuration.checkpointLocation)
      .options(configuration.extraConfOptions)
      .foreachBatch((df: DataFrame, batchId: Long) => {
        if (!DeltaTable.isDeltaTable(df.sparkSession, configuration.destination)) {
          if (isDirEmptyOrDoesNotExist(df.sparkSession, configuration.destination)) {
            logger.info(s"Destination: ${configuration.destination} is not a delta table. Creating new delta table.")
            df.sparkSession
              .createDataFrame(
                df.sparkSession.sparkContext.emptyRDD[Row],
                df.schema
                  .add(StartDateColumn, TimestampType, nullable = false)
                  .add(EndDateColumn, TimestampType, nullable = true)
                  .add(IsCurrentColumn, BooleanType, nullable = false)
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
            .withColumn(StartDateColumn, col(configuration.timestampColumn))
            .withColumn(EndDateColumn, lit(null))
            .withColumn(IsCurrentColumn, lit(false))
            .withColumn(MergeKeyColumn, lit(null))
        )

        val stagedData = setSCD2Fields(union).drop(MergeKeyColumn)
        val uniqueStagedData = removeDuplicates(stagedData)
        generateDeltaMerge(uniqueStagedData).execute()
      })
      .start()
  }

  private def getPreviousEvents(deltaTable: DeltaTable, uniqueChangesForEachKeyAndTimestamp: DataFrame): DataFrame = {
    deltaTable.toDF.as(OldDataPrefix).join(
      uniqueChangesForEachKeyAndTimestamp.as(NewDataPrefix),
      col(s"$NewDataPrefix.${configuration.keyColumn}").equalTo(col(s"$OldDataPrefix.${configuration.keyColumn}"))
        .and(col(s"$NewDataPrefix.${configuration.timestampColumn}").>=(col(s"$OldDataPrefix.$StartDateColumn")))
        .and(col(s"$NewDataPrefix.${configuration.timestampColumn}").<=(col(s"$OldDataPrefix.$EndDateColumn")))
        .or(
          col(s"$NewDataPrefix.${configuration.keyColumn}").equalTo(col(s"$OldDataPrefix.${configuration.keyColumn}"))
            .and(col(s"$NewDataPrefix.${configuration.timestampColumn}").>=(col(s"$OldDataPrefix.$StartDateColumn")))
            .and(col(s"$OldDataPrefix.$IsCurrentColumn").equalTo(true))
        )
    ).select(s"$OldDataPrefix.*").withColumn(s"$MergeKeyColumn", col(s"${configuration.keyColumn}"))
  }

  private def getNextEvents(deltaTable: DeltaTable, uniqueChangesForEachKeyAndTimestamp: DataFrame): DataFrame = {
    val fieldNames = deltaTable.toDF.schema.fieldNames
      .filter(_ != StartDateColumn)
      .filter(_ != configuration.timestampColumn)
    val originalFieldNames = deltaTable.toDF.schema.fieldNames
    deltaTable.toDF.as(OldDataPrefix).join(
      uniqueChangesForEachKeyAndTimestamp.as(NewDataPrefix),
      col(s"$NewDataPrefix.${configuration.keyColumn}").equalTo(col(s"$OldDataPrefix.${configuration.keyColumn}"))
        .and(col(s"$NewDataPrefix.${configuration.timestampColumn}").<(col(s"$OldDataPrefix.$StartDateColumn")))
    ).select(s"$OldDataPrefix.*", s"$NewDataPrefix.${configuration.timestampColumn}")
      .selectExpr(
        s"${configuration.keyColumn}",
        s"$NewDataPrefix.${configuration.timestampColumn}",
        s"struct($StartDateColumn, $OldDataPrefix.${configuration.timestampColumn}, ${fieldNames.mkString(",")}) as otherCols"
      )
      .groupBy(s"${configuration.keyColumn}", s"$NewDataPrefix.${configuration.timestampColumn}")
      .agg(functions.min("otherCols").as("latest"))
      .filter(col("latest").isNotNull)
      .withColumn("latest", new Column(AssertNotNull(col("latest").expr)))
      .selectExpr("latest.*")
      .select(originalFieldNames.head, originalFieldNames.tail: _*)
      .withColumn(s"$MergeKeyColumn", col(s"${configuration.keyColumn}"))
  }

  private def generateDeltaMerge(latestChanges: DataFrame): DeltaMergeBuilder = {
    DeltaTable
      .forPath(configuration.destination)
      .as("currentTable")
      .merge(
        latestChanges.as("changes"), s"currentTable.${configuration.keyColumn} = changes.${configuration.keyColumn} AND currentTable.$StartDateColumn = changes.$StartDateColumn"
      )
      .whenMatched()
      .updateAll()
      .whenNotMatched()
      .insertAll()
  }

  private def setSCD2Fields(dataFrame: DataFrame): DataFrame = {
    val idWindowDesc = org.apache.spark.sql.expressions.Window
      .partitionBy(configuration.keyColumn)
      .orderBy(col(configuration.timestampColumn).desc, col(MergeKeyColumn).desc)
    dataFrame
      .withColumn(
        StartDateColumn,
        col(configuration.timestampColumn)
      )

      .withColumn(
        EndDateColumn,
        functions.when(
          col(MergeKeyColumn).isNotNull.and(
            lag(MergeKeyColumn, 1, null).over(idWindowDesc).isNotNull
          ),
          col(EndDateColumn)
        ).when(
          col(MergeKeyColumn).isNotNull.and(
            lag(MergeKeyColumn, 1, null).over(idWindowDesc).isNull
          ).and(
            col(configuration.timestampColumn).equalTo(
              lag(s"${configuration.timestampColumn}", 1, null).over(idWindowDesc)
            )
          ),
          lag(StartDateColumn, 2, null).over(idWindowDesc)
        ).otherwise(
          lag(StartDateColumn, 1, null).over(idWindowDesc)
        )
      )
      .withColumn(
        EndDateColumn,
        functions
          .when(col(configuration.operationColumn).isInCollection(configuration.operationDeleteValues), col(StartDateColumn))
          .when(!col(configuration.operationColumn).isInCollection(configuration.operationDeleteValues), col(EndDateColumn))
          .otherwise(null)
      )
      .withColumn(
        IsCurrentColumn,
        functions.when(col(EndDateColumn).isNull, lit(true)).otherwise(lit(false))
      )
  }

  private def removeDuplicates(inputDF: DataFrame): DataFrame = {
    val dataFrameWithSortColumns = getDataFrameWithSortColumns(inputDF, SortFieldPrefix)

    val originalFieldNames = inputDF.schema.fieldNames.mkString(",")
    val sortColumnsWithPrefix = configuration.precombineColumns.map(precombineColumn =>
      s"$SortFieldPrefix$precombineColumn"
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
          val orderString = o.mkString(StringSeparator)
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
