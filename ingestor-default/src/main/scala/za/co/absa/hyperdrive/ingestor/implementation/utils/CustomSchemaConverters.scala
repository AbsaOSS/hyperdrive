
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

import org.apache.avro.LogicalTypes.{Date, Decimal, TimestampMicros, TimestampMillis}
import org.apache.avro.Schema.Field
import org.apache.avro.Schema.Type._
import org.apache.avro.SchemaBuilder.{FieldAssembler, FieldBuilder}
import org.apache.avro.util.internal.JacksonUtils
import org.apache.avro.{JsonProperties, LogicalTypes, Schema, SchemaBuilder}
import org.apache.spark.sql.types.Decimal.minBytesForPrecision
import org.apache.spark.sql.types._
import org.codehaus.jackson.map.{ObjectMapper, ObjectReader}

import java.io.{BufferedOutputStream, ByteArrayOutputStream}
import java.util.Objects
import scala.collection.JavaConverters._
import scala.util.Try

// scalastyle:off
object CustomSchemaConverters {
  private lazy val nullSchema = Schema.create(Schema.Type.NULL)
  private val primitiveType = "primitiveType"
  private val defaultValueKey = "default"
  private lazy val objectMapper = new ObjectMapper()

  case class SchemaType(dataType: DataType, nullable: Boolean, primitiveType: Option[Schema])

  /**
   * This function takes an avro schema and returns a sql schema.
   */
  def toSqlType(avroSchema: Schema): DataType = {
    toSqlTypeHelper(avroSchema, Set.empty).dataType
  }

  def toSqlTypeHelper(avroSchema: Schema, existingRecordNames: Set[String]): SchemaType = {
    avroSchema.getType match {
      case INT => avroSchema.getLogicalType match {
        case _: Date => SchemaType(DateType, nullable = false, Option(avroSchema))
        case _ => SchemaType(IntegerType, nullable = false, Option(avroSchema))
      }
      case STRING => SchemaType(StringType, nullable = false, Option(avroSchema))
      case BOOLEAN => SchemaType(BooleanType, nullable = false, Option(avroSchema))
      case BYTES | FIXED => avroSchema.getLogicalType match {
        // For FIXED type, if the precision requires more bytes than fixed size, the logical
        // type will be null, which is handled by Avro library.
        case d: Decimal => SchemaType(DecimalType(d.getPrecision, d.getScale), nullable = false, Option(avroSchema))
        case _ => SchemaType(BinaryType, nullable = false, Option(avroSchema))
      }

      case DOUBLE => SchemaType(DoubleType, nullable = false, Option(avroSchema))
      case FLOAT => SchemaType(FloatType, nullable = false, Option(avroSchema))
      case LONG => avroSchema.getLogicalType match {
        case _: TimestampMillis | _: TimestampMicros => SchemaType(TimestampType, nullable = false, Option(avroSchema))
        case _ => SchemaType(LongType, nullable = false, Option(avroSchema))
      }

      case ENUM => SchemaType(StringType, nullable = false, Option(avroSchema))

      case RECORD =>
        if (existingRecordNames.contains(avroSchema.getFullName)) {
          throw new IncompatibleSchemaException(s"""
                                                   |Found recursive reference in Avro schema, which can not be processed by Spark:
                                                   |${avroSchema.toString(true)}
          """.stripMargin)
        }
        val newRecordNames = existingRecordNames + avroSchema.getFullName
        val fields = avroSchema.getFields.asScala.map { f =>
          val metadataBuilder = new MetadataBuilder()
          val defaultJsonOpt = Option(JacksonUtils.toJsonNode(f.defaultVal()))
          val metadataBuilderWithDefault = defaultJsonOpt match {
            case Some(defaultJson) =>
              val baos = new ByteArrayOutputStream()
              objectMapper.writeValue(baos, defaultJson)
              val r = metadataBuilder.putString(defaultValueKey, baos.toString)
              baos.close()
              r
            case None => metadataBuilder
          }

          val schemaType = toSqlTypeHelper(f.schema(), newRecordNames)
          schemaType.primitiveType
            .map(_.toString)
            .map(schema => metadataBuilderWithDefault.putString(primitiveType, schema).build())
            .map(metadata => StructField(f.name, schemaType.dataType, schemaType.nullable, metadata))
            .getOrElse(StructField(f.name, schemaType.dataType, schemaType.nullable, metadataBuilderWithDefault.build()))
        }

        SchemaType(StructType(fields), nullable = false, None)

      case ARRAY =>
        val schemaType = toSqlTypeHelper(avroSchema.getElementType, existingRecordNames)
        SchemaType(
          ArrayType(schemaType.dataType, containsNull = schemaType.nullable),
          nullable = false,
          schemaType.primitiveType)

      case MAP =>
        val schemaType = toSqlTypeHelper(avroSchema.getValueType, existingRecordNames)
        SchemaType(
          MapType(StringType, schemaType.dataType, valueContainsNull = schemaType.nullable),
          nullable = false, schemaType.primitiveType)

      case UNION =>
        if (avroSchema.getTypes.asScala.exists(_.getType == NULL)) {
          // In case of a union with null, eliminate it and make a recursive call
          val remainingUnionTypes = avroSchema.getTypes.asScala.filterNot(_.getType == NULL)
          if (remainingUnionTypes.size == 1) {
            toSqlTypeHelper(remainingUnionTypes.head, existingRecordNames).copy(nullable = true)
          } else {
            toSqlTypeHelper(Schema.createUnion(remainingUnionTypes.asJava), existingRecordNames)
              .copy(nullable = true)
          }
        } else avroSchema.getTypes.asScala.map(_.getType) match {
          case Seq(t1) =>
            toSqlTypeHelper(avroSchema.getTypes.get(0), existingRecordNames)
          case Seq(t1, t2) if Set(t1, t2) == Set(INT, LONG) =>
            SchemaType(LongType, nullable = false, None)
          case Seq(t1, t2) if Set(t1, t2) == Set(FLOAT, DOUBLE) =>
            SchemaType(DoubleType, nullable = false, None)
          case _ =>
            // Convert complex unions to struct types where field names are member0, member1, etc.
            // This is consistent with the behavior when converting between Avro and Parquet.
            val fields = avroSchema.getTypes.asScala.zipWithIndex.map {
              case (s, i) =>
                val schemaType = toSqlTypeHelper(s, existingRecordNames)
                schemaType.primitiveType
                  .map(_.toString)
                  .map(schema => new MetadataBuilder().putString(primitiveType, schema).build())
                  .map(metadata => StructField(s"member$i", schemaType.dataType, schemaType.nullable, metadata))
                   // All fields are nullable because only one of them is set at a time
                  .getOrElse(StructField(s"member$i", schemaType.dataType, nullable = true))
            }

            SchemaType(StructType(fields), nullable = false, None)
        }

      case other => throw new IncompatibleSchemaException(s"Unsupported type $other")
    }
  }

  def toAvroType(
    catalystType: DataType,
    avroSchema: Option[Schema],
    nullable: Boolean = false,
    defaultValue: Option[Object] = None,
    recordName: String = "topLevelRecord",
    nameSpace: String = "")
  : Schema = {
    val builder = SchemaBuilder.builder()

    val schema = catalystType match {
      case BooleanType => builder.booleanType()
      case ByteType | ShortType | IntegerType => builder.intType()
      case LongType => builder.longType()
      case DateType =>
        LogicalTypes.date().addToSchema(builder.intType())
      case TimestampType => avroSchema match {
        case Some(schema) if schema.getLogicalType.isInstanceOf[TimestampMillis] =>
          LogicalTypes.timestampMillis().addToSchema(builder.longType())
        case _ => LogicalTypes.timestampMicros().addToSchema(builder.longType())
      }
      case FloatType => builder.floatType()
      case DoubleType => builder.doubleType()
      case StringType => builder.stringType()
      case d: DecimalType => avroSchema match {
        case Some(schema) if schema.getType == BYTES =>
          val avroType = LogicalTypes.decimal(d.precision, d.scale)
          avroType.addToSchema(SchemaBuilder.builder().bytesType())
        case _ => getDecimalFixedType(d, avroSchema, nameSpace, recordName)
      }
      case BinaryType => avroSchema match {
        case Some(schema) if schema.getType == FIXED =>
          // Need to avoid naming conflict for the fixed fields
          val name = nameSpace match {
            case "" => s"$recordName.fixed"
            case _ => s"$nameSpace.$recordName.fixed"
          }
          builder
            .fixed(name)
            .size(schema.getFixedSize)
        case _ => builder.bytesType()
      }
      case ArrayType(et, containsNull) =>
        builder.array()
          .items(toAvroType(et, avroSchema, containsNull, None, recordName, nameSpace))
      case MapType(StringType, vt, valueContainsNull) =>
        builder.map()
          .values(toAvroType(vt, avroSchema, valueContainsNull, None, recordName, nameSpace))
      case st: StructType =>
        val childNameSpace = if (nameSpace != "") s"$nameSpace.$recordName" else recordName
        val fieldsAssembler = builder.record(recordName).namespace(nameSpace).fields()
        st.foreach { f =>
          val schema = Try(f.metadata.getString(primitiveType)).toOption
            .map(schema => new Schema.Parser().parse(schema))
          val defaultValueOpt = Try(f.metadata.getString(defaultValueKey)).flatMap(defaultJsonString => Try {
            val jsonNode = objectMapper.readTree(defaultJsonString)
            JacksonUtils.toObject(jsonNode)
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

      // This should never happen.
      case other => throw new IncompatibleSchemaException(s"Unexpected type $other.")
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

  private def getDecimalFixedType(d: DecimalType, avroSchema: Option[Schema], nameSpace: String, recordName: String) = {
    val avroType = LogicalTypes.decimal(d.precision, d.scale)
    avroSchema.map(schema =>
      avroType.addToSchema(SchemaBuilder.fixed(schema.getName).namespace(schema.getNamespace).size(schema.getFixedSize))
    ).getOrElse {
      val fixedSize = minBytesForPrecision(d.precision)
      // Need to avoid naming conflict for the fixed fields
      val name = nameSpace match {
        case "" => s"$recordName.fixed"
        case _ => s"$nameSpace.$recordName.fixed"
      }
      avroType.addToSchema(SchemaBuilder.fixed(name).size(fixedSize))
    }
  }
}

class IncompatibleSchemaException(msg: String, ex: Throwable = null) extends Exception(msg, ex)

