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

import za.co.absa.hyperdrive.ingestor.api.{HasComponentAttributes, PropertyMetadata}
import za.co.absa.hyperdrive.shared.configurations.ConfigurationsKeys.ColumnSelectorStreamTransformerKeys.KEY_COLUMNS_TO_SELECT

trait ColumnSelectorStreamTransformerAttributes extends HasComponentAttributes {

  override def getName: String = "Column Selector Transformer"

  override def getDescription: String = "This transformer selects only the given columns. Column expressions are not possible"

  override def getProperties: Map[String, PropertyMetadata] = Map(
    KEY_COLUMNS_TO_SELECT -> PropertyMetadata("Columns to select", Some("Comma separated list of columns that should be selected. If empty, all columns are selected."), required = false)
  )
}
