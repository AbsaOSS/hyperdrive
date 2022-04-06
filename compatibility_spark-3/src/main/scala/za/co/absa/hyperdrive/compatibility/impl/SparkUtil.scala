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

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.avro.util.internal.JacksonUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.{FileStreamSink, MetadataLogFileIndex}
import za.co.absa.hyperdrive.compatibility.api.CompatibleSparkUtil

import java.io.ByteArrayOutputStream

object SparkUtil extends CompatibleSparkUtil {
  private lazy val objectMapper = new ObjectMapper()

  override def createMetadataLogFileIndex(spark: SparkSession, destination: String): MetadataLogFileIndex =
    new MetadataLogFileIndex(spark, new Path(destination), Map.empty, None)

  override def hasMetadata(spark: SparkSession, destination: String): Boolean =
    FileStreamSink.hasMetadata(Seq(destination), spark.sparkContext.hadoopConfiguration, spark.sessionState.conf)

  override def jsonStringToObject(jsonString: String): Object = {
    val jsonNode = objectMapper.readTree(jsonString)
    JacksonUtils.toObject(jsonNode)
  }

  override def objectToJsonString(obj: Object): Option[String] = {
    Option(JacksonUtils.toJsonNode(obj))
      .map { json =>
        val baos = new ByteArrayOutputStream()
        objectMapper.writeValue(baos, json)
        baos.toString
      }
  }
}
