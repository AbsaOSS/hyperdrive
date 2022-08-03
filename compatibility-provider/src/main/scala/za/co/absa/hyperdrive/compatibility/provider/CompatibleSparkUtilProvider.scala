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

package za.co.absa.hyperdrive.compatibility.provider

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MetadataLogFileIndex
import za.co.absa.hyperdrive.compatibility.api.CompatibleSparkUtil
import za.co.absa.hyperdrive.compatibility.impl.SparkUtil

object CompatibleSparkUtilProvider extends CompatibleSparkUtil {
  def createMetadataLogFileIndex(spark: SparkSession, destination: String): MetadataLogFileIndex =
    SparkUtil.createMetadataLogFileIndex(spark, destination)

  def hasMetadata(spark: SparkSession, destination: String): Boolean =
    SparkUtil.hasMetadata(spark, destination)

  override def jsonStringToObject(jsonString: String): Object =
    SparkUtil.jsonStringToObject(jsonString)

  override def objectToJsonString(obj: Object): Option[String] =
    SparkUtil.objectToJsonString(obj)
}
