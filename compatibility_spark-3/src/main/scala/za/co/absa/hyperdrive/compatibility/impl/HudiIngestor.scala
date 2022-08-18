
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

import org.apache.hudi.DataSourceWriteOptions.{HIVE_STYLE_PARTITIONING, KEYGENERATOR_CLASS_NAME, PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD, TABLE_NAME}
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.hudi.keygen.ComplexKeyGenerator
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import za.co.absa.hyperdrive.compatibility.api.CompatibleHudiIngestor

object HudiIngestor extends CompatibleHudiIngestor {
  override def upsertStream(df: DataFrame, partitionColumns: Seq[String], destination: String, keyColumn: String,
                            timestampColumn: String): StreamingQuery = {
    val dsw1 = df.writeStream
      .format("hudi")
      .options(getQuickstartWriteConfigs)
      .option(PRECOMBINE_FIELD.key(), timestampColumn)
      .option(RECORDKEY_FIELD.key(), keyColumn)
      .option(KEYGENERATOR_CLASS_NAME.key(), classOf[ComplexKeyGenerator].getName)
      .option(PARTITIONPATH_FIELD.key(), partitionColumns.mkString(","))
      .option(HIVE_STYLE_PARTITIONING.key(), "true")
      .option(TABLE_NAME.key(), destination)
      .option("hoodie.table.name", destination)
      .outputMode(OutputMode.Append())
      .option("checkpointLocation", s"${destination}/_checkpoints")
    dsw1.start(destination)
  }
}
