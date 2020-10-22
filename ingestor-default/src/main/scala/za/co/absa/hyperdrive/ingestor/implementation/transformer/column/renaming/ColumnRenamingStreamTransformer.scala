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

package za.co.absa.hyperdrive.ingestor.implementation.transformer.column.renaming

import org.apache.commons.configuration2.Configuration
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.DataFrame
import za.co.absa.hyperdrive.ingestor.api.transformer.{StreamTransformer, StreamTransformerFactory}

private[transformer] class ColumnRenamingStreamTransformer(val columnsFrom: Seq[String], val columnsTo: Seq[String]) extends StreamTransformer {
  if (columnsFrom.isEmpty || columnsTo.isEmpty) {
    throw new IllegalArgumentException("Empty list of columns to rename.")
  }

  if (columnsFrom.size != columnsTo.size) {
    throw new IllegalArgumentException("The size of source column names doesn't match the list of target column names " +
      s"${columnsFrom.size} != ${columnsTo.size}.")
  }

  override def transform(streamData: DataFrame): DataFrame = {
    val renamings = columnsFrom.zip(columnsTo)

    renamings.foldLeft(streamData){ case (df, (from, to)) =>
      df.withColumnRenamed(from, to)
    }
  }
}

object ColumnRenamingStreamTransformer extends StreamTransformerFactory with ColumnRenamingStreamTransformerAttributes {
  override def apply(config: Configuration): StreamTransformer = {
    val columnsFrom = config.getStringArray(KEY_COLUMNS_FROM)
    val columnsTo = config.getStringArray(KEY_COLUMNS_TO)
    LogManager.getLogger.info(s"Going to create ColumnRenamingStreamTransformer using: " +
      s"columnsFrom='${columnsFrom.mkString(",")}', columnsTo='${columnsTo.mkString(",")}'")
    new ColumnRenamingStreamTransformer(columnsFrom, columnsTo)
  }
}
