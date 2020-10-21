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

package za.co.absa.hyperdrive.ingestor.implementation.transformer.enceladus.columns

import za.co.absa.hyperdrive.ingestor.api.{HasComponentAttributes, PropertyMetadata}

trait AddEnceladusColumnsTransformerAttributes extends HasComponentAttributes {

  val KeyReportDate = "report.date"

  override def getName: String = "Add Enceladus Columns Transformer"

  override def getDescription: String = "This transformer is a surrogate that adds columns which would be added by enceladus: " +
    "enceladus_info_date, enceladus_info_date_string, enceladus_version, errCol"

  override def getProperties: Map[String, PropertyMetadata] = Map(
    KeyReportDate -> PropertyMetadata("Date", Some("Date to add as a column, e.g. 2018-01-23. By default, the current date will be taken."), required = false)
  )
}
