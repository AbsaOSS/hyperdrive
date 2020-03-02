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
package za.co.absa.hyperdrive.ingestor.implementation.transformer.column.selection

import org.apache.commons.configuration2.Configuration
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.DataFrame
import za.co.absa.hyperdrive.ingestor.api.PropertyMetadata
import za.co.absa.hyperdrive.ingestor.api.transformer.{StreamTransformer, StreamTransformerFactory, StreamTransformerFactoryProvider}
import za.co.absa.hyperdrive.shared.configurations.ConfigurationsKeys.ColumnSelectorStreamTransformerKeys.KEY_COLUMNS_TO_SELECT

private[transformer] class ColumnSelectorStreamTransformer(val columns: Seq[String]) extends StreamTransformer {

  if (columns == null || columns.isEmpty) {
    throw new IllegalArgumentException("Empty list of columns to select.")
  }

  def transform(streamData: DataFrame): DataFrame = {
    if (streamData == null) {
      throw new IllegalArgumentException("Null DataFrame received.")
    }

    streamData.select(columns.head, columns.tail:_*)
  }
}

object ColumnSelectorStreamTransformer extends StreamTransformerFactory with ColumnSelectorStreamTransformerAttributes {
  override def apply(config: Configuration): StreamTransformer = {
    val columns = getColumnsAsSequence(config)
    LogManager.getLogger.info(s"Going to create ColumnSelectorStreamTransformer using: columns='$columns'")
    new ColumnSelectorStreamTransformer(columns)
  }

  private def getColumnsAsSequence(configuration: Configuration): Seq[String] = {
    configuration.getStringArray(KEY_COLUMNS_TO_SELECT) match {
      case array if array.nonEmpty => array
      case _ => Seq("*")
    }
  }
}
