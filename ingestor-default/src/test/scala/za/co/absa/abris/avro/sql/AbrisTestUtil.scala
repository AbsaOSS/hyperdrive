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

package za.co.absa.abris.avro.sql

import org.apache.spark.sql.functions.lit
import za.co.absa.abris.avro.functions.{from_avro, to_avro}
import za.co.absa.abris.config.{FromAvroConfig, ToAvroConfig}

object AbrisTestUtil {
  def getSchemaRegistryConf(fromAvroConfig: FromAvroConfig): Option[Map[String, String]] = {
    from_avro(lit(1), fromAvroConfig).expr match {
      case wrapper: AvroDataToCatalyst => wrapper.schemaRegistryConf
      case _ => throw new IllegalArgumentException("Test internal exception. Either the product index or the type of FromAvroConfig.schemaRegistryConf is wrong")
    }
  }

  def getFromSchemaString(fromAvroConfig: FromAvroConfig): String = getAbrisConfig(fromAvroConfig)("readerSchema").asInstanceOf[String]

  def getAbrisConfig(fromAvroConfig: FromAvroConfig): Map[String, Any] = {
    from_avro(lit(1), fromAvroConfig).expr match {
      case wrapper: AvroDataToCatalyst => wrapper.abrisConfig
      case _ => throw new IllegalArgumentException("Test internal exception. Either the product index or the type of FromAvroConfig.abrisConfig is wrong")
    }
  }
  def getToSchemaString(toAvroConfig: ToAvroConfig): String =
    getAbrisConfig(toAvroConfig)("schema").asInstanceOf[String]

  def getSchemaId(toAvroConfig: ToAvroConfig): Option[Int] =
    getAbrisConfig(toAvroConfig).get("schemaId").map(_.asInstanceOf[Int])

  private def getAbrisConfig(toAvroConfig: ToAvroConfig): Map[String, Any] = {
    to_avro(lit(1), toAvroConfig).expr match {
      case wrapper: CatalystDataToAvro => wrapper.abrisConfig
      case _ => throw new IllegalArgumentException("Test internal exception. Either the product index or the type of FromAvroConfig.abrisConfig is wrong")
    }
  }
}
