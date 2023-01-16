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

package za.co.absa.hyperdrive.compatibility.impl.writer.cdc.delta

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory
import za.co.absa.hyperdrive.compatibility.impl.writer.cdc.CDCUtil.isDirEmptyOrDoesNotExist

object DeltaUtil {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private[hyperdrive] def createDeltaTableIfNotExists(sparkSession: SparkSession, destination: String, schema: StructType, partitionColumns: Seq[String]): Unit = {
    if (!DeltaTable.isDeltaTable(sparkSession, destination)) {
      if (isDirEmptyOrDoesNotExist(sparkSession, destination)) {
        logger.info(s"Destination: $destination is not a delta table. Creating new delta table.")
        sparkSession
          .createDataFrame(sparkSession.sparkContext.emptyRDD[Row], schema)
          .write
          .format("delta")
          .mode(SaveMode.Overwrite)
          .option("overwriteSchema", "true")
          .partitionBy(partitionColumns: _*)
          .save(destination)
      } else {
        throw new IllegalArgumentException(s"Could not create new delta table. Directory $destination is not empty!")
      }
    }
  }
}
