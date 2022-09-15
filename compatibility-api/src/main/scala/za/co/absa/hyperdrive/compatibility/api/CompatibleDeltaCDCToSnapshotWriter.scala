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

package za.co.absa.hyperdrive.compatibility.api

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

trait CompatibleDeltaCDCToSnapshotWriter {
  def write(dataFrame: DataFrame): StreamingQuery
}

case class DeltaCDCToSnapshotWriterConfiguration(
  checkpointLocationPropName: String,
  destination: String,
  trigger: Trigger,
  checkpointLocation: String,
  partitionColumns: Seq[String],
  keyColumn: String,
  opColumn: String,
  deletedValue: String,
  sortColumns: Seq[String],
  sortColumnsCustomOrder: Map[String, Seq[String]],
  extraConfOptions: Map[String, String]
)
