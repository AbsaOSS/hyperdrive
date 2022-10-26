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

import za.co.absa.hyperdrive.compatibility.api.{CompatibleDeltaCDCToSnapshotWriter, DeltaCDCToSnapshotWriterConfiguration}
import io.delta.tables.{DeltaMergeBuilder, DeltaTable}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{Column, DataFrame, Row, SaveMode, SparkSession, functions}
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.slf4j.LoggerFactory
import za.co.absa.hyperdrive.shared.utils.FileUtils

import java.net.URI

class DeltaCDCToSnapshotWriter(configuration: DeltaCDCToSnapshotWriterConfiguration) extends CompatibleDeltaCDCToSnapshotWriter {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val STRING_SEPARATOR = "#$@"
  private val CHECKPOINT_LOCATION = "checkpointLocation"

  if(configuration.precombineColumnsCustomOrder.values.flatten.toSeq.contains(STRING_SEPARATOR)) {
    throw new IllegalArgumentException(s"Precombine columns custom order cannot contain string separator: $STRING_SEPARATOR")
  }

  override def write(dataFrame: DataFrame): StreamingQuery = {
    if (!DeltaTable.isDeltaTable(dataFrame.sparkSession, configuration.destination)) {
      if (isDirEmptyOrDoesNotExist(dataFrame.sparkSession, configuration.destination)) {
        logger.info(s"Destination: ${configuration.destination} is not a delta table. Creating new delta table.")
        dataFrame.sparkSession
          .createDataFrame(dataFrame.sparkSession.sparkContext.emptyRDD[Row], dataFrame.schema)
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

    dataFrame.writeStream
      .trigger(configuration.trigger)
      .outputMode(OutputMode.Append())
      .option(CHECKPOINT_LOCATION, configuration.checkpointLocation)
      .options(configuration.extraConfOptions)
      .foreachBatch((df: DataFrame, batchId: Long) => {
        logger.info(s"Writing batchId: $batchId")

        val sortFieldsPrefix = "_tmp_hyperdrive_"

        val dataFrameWithSortColumns = getDataFrameWithSortColumns(df, sortFieldsPrefix)

        val originalFieldNames = df.schema.fieldNames.mkString(",")
        val sortColumnsWithPrefix = configuration.precombineColumns.map(precombineColumn => s"$sortFieldsPrefix$precombineColumn")

        val latestChangeForEachKey = dataFrameWithSortColumns
          .selectExpr(s"${configuration.keyColumn}", s"struct(${sortColumnsWithPrefix.mkString(",")}, $originalFieldNames) as otherCols" )
          .groupBy(s"${configuration.keyColumn}")
          .agg(functions.max("otherCols").as("latest"))
          .filter(col("latest").isNotNull)
          .withColumn("latest", new Column(AssertNotNull(col("latest").expr)))
          .selectExpr("latest.*")
          .drop(sortColumnsWithPrefix :_*)

        generateDeltaMerge(latestChangeForEachKey).execute()
      })
      .start()
  }

  private def generateDeltaMerge(latestChanges: DataFrame): DeltaMergeBuilder = {
    val initialDeltaBuilder = DeltaTable
      .forPath(configuration.destination)
      .as("currentTable")
      .merge(latestChanges.as("changes"), s"currentTable.${configuration.keyColumn} = changes.${configuration.keyColumn}")
      .whenMatched(when(col(s"changes.${configuration.operationColumn}").isInCollection(configuration.operationDeleteValues), true).otherwise(false))
      .delete()

    val deltaBuilderWithSortColumns = configuration.precombineColumns.foldLeft(initialDeltaBuilder) { (builder, precombineColumn) =>
      val order = configuration.precombineColumnsCustomOrder.getOrElse(precombineColumn, Seq.empty[String])
      order match {
        case o if o.isEmpty =>
          builder
            .whenMatched(s"changes.$precombineColumn > currentTable.$precombineColumn")
            .updateAll()
        case o =>
          val orderString = o.mkString(STRING_SEPARATOR)
          builder
            .whenMatched(s"""locate(changes.$precombineColumn, "$orderString") > locate(currentTable.$precombineColumn, "$orderString")""")
            .updateAll()
      }
    }

    deltaBuilderWithSortColumns
      .whenNotMatched(when(col(s"changes.${configuration.operationColumn}").isInCollection(configuration.operationDeleteValues), false).otherwise(true))
      .insertAll()
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
