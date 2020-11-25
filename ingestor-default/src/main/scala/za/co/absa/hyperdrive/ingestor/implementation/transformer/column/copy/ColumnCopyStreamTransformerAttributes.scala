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

package za.co.absa.hyperdrive.ingestor.implementation.transformer.column.copy

import za.co.absa.hyperdrive.ingestor.api.{HasComponentAttributes, PropertyMetadata}

trait ColumnCopyStreamTransformerAttributes extends HasComponentAttributes {
  val KEY_COLUMNS_FROM = "columns.copy.from"
  val KEY_COLUMNS_TO = "columns.copy.to"

  override def getName: String = "Column Copy Transformer"

  override def getDescription: String = "This transformer copies given columns. Column expressions are not possible"

  override def getProperties: Map[String, PropertyMetadata] = Map(
    KEY_COLUMNS_FROM -> PropertyMetadata("Source column names", Some("Comma separated list of columns to be copied."), required = true),
    KEY_COLUMNS_TO -> PropertyMetadata("Target column names", Some("Comma separated list of new names of the columns. The number of columns should match the list of source columns."), required = true)
  )
}
