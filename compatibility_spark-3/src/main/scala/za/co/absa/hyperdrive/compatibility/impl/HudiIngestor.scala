
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

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.hudi.keygen.ComplexKeyGenerator
import za.co.absa.hyperdrive.compatibility.api.CompatibleHudiIngestor

object HudiIngestor extends CompatibleHudiIngestor {
  def getWriteConfigs(partitionColumns: Seq[String], destination: String, keyColumn: String,
                      timestampColumn: String): Map[String, String] = {
    import scala.collection.JavaConverters._
    getQuickstartWriteConfigs.asScala.toMap ++
      Map(
        PRECOMBINE_FIELD.key() -> timestampColumn,
        RECORDKEY_FIELD.key() -> keyColumn,
        KEYGENERATOR_CLASS_NAME.key() -> classOf[ComplexKeyGenerator].getName,
        PARTITIONPATH_FIELD.key() -> partitionColumns.mkString(","),
        HIVE_STYLE_PARTITIONING.key() -> "true",
        TABLE_NAME.key() -> destination,
        "hoodie.table.name" -> destination
      )
  }
}
