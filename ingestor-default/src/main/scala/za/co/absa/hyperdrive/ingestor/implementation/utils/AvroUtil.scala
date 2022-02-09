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

import scala.util.{Failure, Success, Try}

private[hyperdrive] object AvroUtil {

  def getFromConsumerRecord(record: ConsumerRecord[GenericRecord, GenericRecord], fieldName: String): Option[Any] = {
    val fieldValue = fieldName match {
      case "topic" => Option(record.topic())
      case "offset" => Option(record.offset())
      case "partition" => Option(record.partition())
      case "timestamp" => Option(record.timestamp())
      case "timestampType" => Option(record.timestampType())
      case "serializedKeySize" => Option(record.serializedKeySize())
      case "serializedValueSize" => Option(record.serializedValueSize())
      case keyColumn if keyColumn.startsWith("key.") => getFromGenericRecordNullSafe(record.key(),
        UnresolvedAttribute.parseAttributeName(keyColumn.stripPrefix("key.")).toList)
      case valueColumn if valueColumn.startsWith("value.") => getFromGenericRecordNullSafe(record.value(),
        UnresolvedAttribute.parseAttributeName(valueColumn.stripPrefix("value.")).toList)
      case _ => None
    }

    fieldValue.map {
      case utf8: Utf8 => utf8.toString
      case v => v
    }
  }

  private def getFromGenericRecordNullSafe(record: GenericRecord, keys: Seq[String]) =
    Option(record).flatMap(getFromGenericRecord(_, keys))

  private def getFromGenericRecord(record: GenericRecord, keys: Seq[String]): Option[Any] = keys match {
    case key :: Nil => getValueSafely(() => record.get(key))
    case head :: tail =>
      getValueSafely(() => record.get(head)).flatMap {
        case genericRecord: GenericRecord => getFromGenericRecord(genericRecord, tail)
        case _ => None
      }
  }

  private def getValueSafely[T](fn: () => T): Option[T] = {
    Try(fn.apply()) match {
      case Success(value) => Option(value)
      case Failure(_) => None
    }
  }
}
