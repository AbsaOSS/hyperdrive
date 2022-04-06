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

package za.co.absa.hyperdrive.ingestor.implementation.transformer.avro.confluent

import org.apache.avro.LogicalTypes.TimestampMillis
import org.apache.avro.Schema.Type._
import org.apache.avro.{JsonProperties, LogicalTypes, Schema, SchemaBuilder}
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.types.Decimal.minBytesForPrecision
import org.apache.spark.sql.types._
import za.co.absa.hyperdrive.compatibility.provider.CompatibleSparkUtilProvider

import java.util.Objects
import scala.util.Try
import za.co.absa.hyperdrive.ingestor.implementation.transformer.avro.confluent.SparkMetadataKeys._

object AdvancedSparkToAvroConverter extends SparkToAvroConverter {
  private lazy val nullSchema = Schema.create(Schema.Type.NULL)

  override def apply(catalystType: DataType, nullable: Boolean, recordName: String, nameSpace: String): Schema =
    toAvroType(catalystType, None, nullable, None, recordName, nameSpace)

  // scalastyle:off
  private def toAvroType(
    catalystType: DataType,
    avroSchema: Option[Schema],
    nullable: Boolean = false,
    defaultValue: Option[Object] = None,
    recordName: String = "topLevelRecord",
    nameSpace: String = "")
  : Schema = {
    val builder = SchemaBuilder.builder()

    val schema = catalystType match {
      case TimestampType => avroSchema match {
        case Some(schema) if schema.getLogicalType.isInstanceOf[TimestampMillis] =>
          LogicalTypes.timestampMillis().addToSchema(builder.longType())
        case _ => LogicalTypes.timestampMicros().addToSchema(builder.longType())
      }
      case d: DecimalType => avroSchema match {
        case Some(schema) if schema.getType == BYTES =>
          val avroType = LogicalTypes.decimal(d.precision, d.scale)
          avroType.addToSchema(SchemaBuilder.builder().bytesType())
        case _ => getDecimalFixedType(d, avroSchema, nameSpace, recordName)
      }
      case BinaryType => avroSchema match {
        case Some(schema) if schema.getType == FIXED =>
          val name = getFixedName(recordName, nameSpace)
          builder
            .fixed(name)
            .size(schema.getFixedSize)
        case _ => builder.bytesType()
      }
      case ArrayType(et, containsNull) =>
        builder.array()
          .items(toAvroType(et, avroSchema, containsNull, defaultValue, recordName, nameSpace))
      case MapType(StringType, vt, valueContainsNull) =>
        builder.map()
          .values(toAvroType(vt, avroSchema, valueContainsNull, defaultValue, recordName, nameSpace))
      case st: StructType =>
        val childNameSpace = if (nameSpace != "") s"$nameSpace.$recordName" else recordName
        val fieldsAssembler = builder.record(recordName).namespace(nameSpace).fields()
        st.foreach { f =>
          val schema = Try(f.metadata.getString(AvroTypeKey)).toOption
            .map(schema => new Schema.Parser().parse(schema))
          val defaultValueOpt = Try(f.metadata.getString(DefaultValueKey))
            .flatMap(defaultJsonString => Try {
            CompatibleSparkUtilProvider.jsonStringToObject(defaultJsonString)
          }).toOption
          val fieldAvroType =
            toAvroType(f.dataType, schema, f.nullable, defaultValueOpt, f.name, childNameSpace)
          defaultValueOpt match {
            case Some(defaultObject) if !Objects.equals(defaultObject, JsonProperties.NULL_VALUE) =>
              fieldsAssembler.name(f.name).`type`(fieldAvroType).withDefault(defaultObject)
            case Some(_) =>
              fieldsAssembler.name(f.name).`type`(fieldAvroType).withDefault(null)
            case _ => fieldsAssembler.name(f.name).`type`(fieldAvroType).noDefault()
          }
        }
        fieldsAssembler.endRecord()

      // nullability is handled later in this method, thus pass nullable = false
      case _ => SchemaConverters.toAvroType(catalystType, nullable = false, recordName, nameSpace)
    }
    if (nullable) {
      defaultValue match {
        case Some(value) if !value.isInstanceOf[JsonProperties.Null] => Schema.createUnion(schema, nullSchema)
        case _ => Schema.createUnion(nullSchema, schema)
      }
    } else {
      schema
    }
  }

  // scalastyle:on
  private def getDecimalFixedType(d: DecimalType, avroSchema: Option[Schema], nameSpace: String, recordName: String) = {
    val avroType = LogicalTypes.decimal(d.precision, d.scale)
    val name = getFixedName(recordName, nameSpace)
    val minBytes = minBytesForPrecision(d.precision)
    val size = avroSchema.map { schema =>
      if (schema.getFixedSize > minBytes) schema.getFixedSize else minBytes
    }.getOrElse {
      minBytes
    }
    avroType.addToSchema(SchemaBuilder.fixed(name).size(size))
  }

  private def getFixedName(recordName: String, nameSpace: String) = {
    // Need to avoid naming conflict for the fixed fields
    nameSpace match {
      case "" => s"$recordName.fixed"
      case _ => s"$nameSpace.$recordName.fixed"
    }
  }
}
