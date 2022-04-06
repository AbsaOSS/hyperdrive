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

import org.apache.avro.Schema
import org.apache.avro.Schema.Type._
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.types._
import za.co.absa.abris.avro.sql.SchemaConverter
import za.co.absa.hyperdrive.compatibility.provider.CompatibleSparkUtilProvider
import za.co.absa.hyperdrive.ingestor.implementation.transformer.avro.confluent.SparkMetadataKeys._

import scala.collection.JavaConverters._

// scalastyle:off
class AdvancedAvroToSparkConverter extends SchemaConverter {
  override val shortName: String = AdvancedAvroToSparkConverter.name

  case class SchemaType(dataType: DataType, nullable: Boolean, avroType: Option[Schema])

  /**
   * This function takes an avro schema and returns a sql schema.
   */
  override def toSqlType(avroSchema: Schema): DataType = {
    toSqlTypeHelper(avroSchema, Set.empty).dataType
  }

  def toSqlTypeHelper(avroSchema: Schema, existingRecordNames: Set[String]): SchemaType = {
    avroSchema.getType match {
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
          val defaultJsonOpt = CompatibleSparkUtilProvider.objectToJsonString(f.defaultVal())
          val metadataBuilderWithDefault = defaultJsonOpt match {
            case Some(defaultJson) => metadataBuilder.putString(DefaultValueKey, defaultJson)
            case None => metadataBuilder
          }

          val schemaType = toSqlTypeHelper(f.schema(), newRecordNames)
          schemaType.avroType
            .map(_.toString)
            .map(schema => metadataBuilderWithDefault.putString(AvroTypeKey, schema).build())
            .map(metadata => StructField(f.name, schemaType.dataType, schemaType.nullable, metadata))
            .getOrElse(StructField(f.name, schemaType.dataType, schemaType.nullable, metadataBuilderWithDefault.build()))
        }

        SchemaType(StructType(fields), nullable = false, None)

      case ARRAY =>
        val schemaType = toSqlTypeHelper(avroSchema.getElementType, existingRecordNames)
        SchemaType(
          ArrayType(schemaType.dataType, containsNull = schemaType.nullable),
          nullable = false,
          schemaType.avroType)

      case MAP =>
        val schemaType = toSqlTypeHelper(avroSchema.getValueType, existingRecordNames)
        SchemaType(
          MapType(StringType, schemaType.dataType, valueContainsNull = schemaType.nullable),
          nullable = false,
          schemaType.avroType)

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
            SchemaType(LongType, nullable = false, Option(avroSchema))
          case Seq(t1, t2) if Set(t1, t2) == Set(FLOAT, DOUBLE) =>
            SchemaType(DoubleType, nullable = false, Option(avroSchema))
          case _ =>
            // Convert complex unions to struct types where field names are member0, member1, etc.
            // This is consistent with the behavior when converting between Avro and Parquet.
            val fields = avroSchema.getTypes.asScala.zipWithIndex.map {
              case (s, i) =>
                val schemaType = toSqlTypeHelper(s, existingRecordNames)
                schemaType.avroType
                  .map(_.toString)
                  .map(schema => new MetadataBuilder().putString(AvroTypeKey, schema).build())
                  .map(metadata => StructField(s"member$i", schemaType.dataType, schemaType.nullable, metadata))
                   // All fields are nullable because only one of them is set at a time
                  .getOrElse(StructField(s"member$i", schemaType.dataType, nullable = true))
            }

            SchemaType(StructType(fields), nullable = false, None)
        }

      case _ =>
        val originalSchemaType = SchemaConverters.toSqlType(avroSchema)
        SchemaType(originalSchemaType.dataType, originalSchemaType.nullable, Option(avroSchema))
    }
  }
}

// scalastyle:on
object AdvancedAvroToSparkConverter {
  val name = "advanced"
}
