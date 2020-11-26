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

package za.co.absa.hyperdrive.ingestor.implementation.utils

import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute

import scala.annotation.tailrec

private[hyperdrive] object AvroUtil {

  def getIdColumnsFromRecord(record: ConsumerRecord[GenericRecord, GenericRecord], idColumnNames: Seq[String]): Seq[Any] = {
    idColumnNames.map {
      case "topic" => record.topic()
      case "offset" => record.offset()
      case "partition" => record.partition()
      case "timestamp" => record.timestamp()
      case "timestampType" => record.timestampType()
      case "serializedKeySize" => record.serializedKeySize()
      case "serializedValueSize" => record.serializedValueSize()
      case "headers" => record.headers()
      case keyColumn if keyColumn.startsWith("key.") => getRecursively(record.value(),
        UnresolvedAttribute.parseAttributeName(keyColumn.stripPrefix("key.")).toList)
      case valueColumn if valueColumn.startsWith("value.") => getRecursively(record.value(),
        UnresolvedAttribute.parseAttributeName(valueColumn.stripPrefix("value.")).toList)
    }.map {
      case utf8: Utf8 => utf8.toString
      case v => v
    }
  }

  @tailrec
  def getRecursively(record: GenericRecord, keys: List[String]): Any = keys match {
    case key :: Nil => record.get(key)
    case head :: tail => getRecursively(record.get(head).asInstanceOf[GenericRecord], tail)
  }
}
