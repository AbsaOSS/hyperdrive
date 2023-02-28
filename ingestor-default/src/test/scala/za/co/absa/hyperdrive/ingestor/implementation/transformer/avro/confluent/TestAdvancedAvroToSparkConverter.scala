
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
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.hyperdrive.ingestor.implementation.transformer.avro.confluent.SparkMetadataKeys.{AvroTypeKey, DefaultValueKey}

import scala.io.Source

class TestAdvancedAvroToSparkConverter extends AnyFlatSpec with Matchers {
  private val underTest = new AdvancedAvroToSparkConverter()

  it should "create the same avro schema for non-nullable types as the default implementation" in {
    val avroSchema = getAvroSchemaFromJson("avro-spark-conversion/non-nullable-types.json")
    val dateType =
      raw"""
           |{
           |  "type" : "int",
           |  "logicalType" : "date"
           |}
           |""".stripMargin.filterNot(_.isWhitespace)
    val timestampType =
      raw"""
           |{
           |  "type" : "long",
           |  "logicalType" : "timestamp-micros"
           |}
           |""".stripMargin.filterNot(_.isWhitespace)
    val decimalType =
      raw"""
           |{
           |  "type" : "fixed",
           |  "name" : "fixed",
           |  "namespace" : "nameSpace.recordName.decimalCol",
           |  "size" : 5,
           |  "logicalType" : "decimal",
           |  "precision" : 10,
           |  "scale" : 3
           |}
           |""".stripMargin.filterNot(_.isWhitespace)

    val expectedSchema = StructType(Seq(
      StructField("booleanCol", BooleanType, nullable = false,
        metadata = new MetadataBuilder().putString(AvroTypeKey, "\"boolean\"").build()),
      StructField("intCol", IntegerType, nullable = false,
        metadata = new MetadataBuilder().putString(AvroTypeKey, "\"int\"").build()),
      StructField("longCol", LongType, nullable = false,
        metadata = new MetadataBuilder().putString(AvroTypeKey, "\"long\"").build()),
      StructField("dateCol", DateType, nullable = false,
        metadata = new MetadataBuilder().putString(AvroTypeKey, dateType).build()),
      StructField("floatCol", FloatType, nullable = false,
        metadata = new MetadataBuilder().putString(AvroTypeKey, "\"float\"").build()),
      StructField("doubleCol", DoubleType, nullable = false,
        metadata = new MetadataBuilder().putString(AvroTypeKey, "\"double\"").build()),
      StructField("stringCol", StringType, nullable = false,
        metadata = new MetadataBuilder().putString(AvroTypeKey, "\"string\"").build()),
      StructField("timestampCol", TimestampType, nullable = false,
        metadata = new MetadataBuilder().putString(AvroTypeKey, timestampType).build()),
      StructField("decimalCol", new DecimalType(10, 3), nullable = false,
        metadata = new MetadataBuilder().putString(AvroTypeKey, decimalType).build()),
      StructField("binaryCol", BinaryType, nullable = false,
        metadata = new MetadataBuilder().putString(AvroTypeKey, "\"bytes\"").build()),
      StructField("arrayCol", ArrayType(StringType, containsNull = false), nullable = false,
        metadata = new MetadataBuilder().putString(AvroTypeKey, "\"string\"").build()),
      StructField("mapCol", MapType(StringType, StringType, valueContainsNull = false), nullable = false,
        metadata = new MetadataBuilder().putString(AvroTypeKey, "\"string\"").build()),
      StructField("nestedCol", StructType(Seq(
          StructField("stringCol", StringType, nullable = false,
            metadata = new MetadataBuilder().putString(AvroTypeKey, "\"string\"").build()))),
        nullable = false)
    ))

    val schema = underTest.toSqlType(avroSchema)

    schema shouldBe expectedSchema
  }

  it should "create a schema for nullable types" in {
    val avroSchema = getAvroSchemaFromJson("avro-spark-conversion/nullable-types-nulls-first.json")
    val expectedSchema = StructType(Seq(
      StructField("booleanCol", BooleanType),
      StructField("byteCol", IntegerType),
      StructField("shortCol", IntegerType),
      StructField("intCol", IntegerType),
      StructField("longCol", LongType),
      StructField("dateCol", DateType),
      StructField("floatCol", FloatType),
      StructField("doubleCol", DoubleType),
      StructField("stringCol", StringType),
      StructField("timestampCol", TimestampType),
      StructField("decimalCol", new DecimalType(10, 3)),
      StructField("binaryCol", BinaryType),
      StructField("arrayCol", ArrayType(StringType)),
      StructField("mapCol", MapType(StringType, StringType)),
      StructField("nestedCol", StructType(Seq(
        StructField("stringCol", StringType),
        StructField("arrayCol", ArrayType(StringType, containsNull = true))
      )))
    ))

    val schema = underTest.toSqlType(avroSchema)

    DataType.equalsStructurally(schema, expectedSchema) shouldBe true
  }

  it should "add default values to metadata" in {
    val avroSchema = getAvroSchemaFromJson("avro-spark-conversion/types-with-defaults.json")
    val expectedSchema = StructType(Seq(
      StructField("stringCol", StringType,
        metadata = new MetadataBuilder()
          .putString(DefaultValueKey, "\"abcd\"")
          .putString(AvroTypeKey, "\"string\"")
          .build()),
      StructField("intCol", IntegerType,
        metadata = new MetadataBuilder()
          .putString(DefaultValueKey, "null")
          .putString(AvroTypeKey, "\"int\"")
          .build()),
      StructField("arrayCol", ArrayType(StringType),
        metadata = new MetadataBuilder()
          .putString(DefaultValueKey, "[\"value1\",\"value2\"]")
          .putString(AvroTypeKey, "\"string\"")
          .build()),
      StructField("arrayNonNullCol", ArrayType(StringType, containsNull = false),
        metadata = new MetadataBuilder()
          .putString(DefaultValueKey, "[\"value1\",\"value2\"]")
          .putString(AvroTypeKey, "\"string\"")
          .build()),
      StructField("mapCol", MapType(StringType, StringType),
        metadata = new MetadataBuilder()
          .putString(DefaultValueKey, "{\"value1\":\"value2\"}")
          .putString(AvroTypeKey, "\"string\"")
          .build()),
      StructField("mapNonNullCol", MapType(StringType, StringType, valueContainsNull = false),
        metadata = new MetadataBuilder()
          .putString(DefaultValueKey, "{\"value1\":\"value2\"}")
          .putString(AvroTypeKey, "\"string\"")
          .build()),
      StructField("nestedCol", StructType(Seq(
        StructField(
          "stringCol",
          StringType,
          metadata = new MetadataBuilder()
            .putString(DefaultValueKey, "\"xyz\"")
            .putString(AvroTypeKey, "\"string\"")
            .build()
        ))))
    ))

    val schema = underTest.toSqlType(avroSchema)

    schema shouldBe expectedSchema
  }

  it should "convert a union with int and long to a long type" in {
    val avroSchemaString =
      raw"""
           |{
           |  "type": "record",
           |  "name": "recordName",
           |  "namespace": "nameSpace",
           |  "fields": [
           |    {
           |      "name": "complexCol",
           |      "type": ["int", "long"]
           |    }
           |  ]
           |}
           |""".stripMargin
    val avroSchema = new Schema.Parser().parse(avroSchemaString)
    val expectedSchema = StructType(Seq(
      StructField("complexCol", LongType, nullable = false,
        metadata = new MetadataBuilder().putString(AvroTypeKey, "[\"int\",\"long\"]").build())
    ))

    val schema = underTest.toSqlType(avroSchema)

    schema shouldBe expectedSchema
  }

  it should "convert a union with float and double to a double type" in {
    val avroSchemaString =
      raw"""
           |{
           |  "type": "record",
           |  "name": "recordName",
           |  "namespace": "nameSpace",
           |  "fields": [
           |    {
           |      "name": "complexCol",
           |      "type": ["null", "float", "double"]
           |    }
           |  ]
           |}
           |""".stripMargin
    val avroSchema = new Schema.Parser().parse(avroSchemaString)
    val expectedSchema = StructType(Seq(
      StructField("complexCol", DoubleType,
        metadata = new MetadataBuilder().putString(AvroTypeKey, "[\"float\",\"double\"]").build())
    ))

    val schema = underTest.toSqlType(avroSchema)

    schema shouldBe expectedSchema
  }

  it should "convert a complex union to struct types with field names member0, member1, etc." in {
    val avroSchemaString =
      raw"""
           |{
           |  "type": "record",
           |  "name": "recordName",
           |  "namespace": "nameSpace",
           |  "fields": [
           |    {
           |      "name": "complexCol",
           |      "type": ["null", "boolean", "int", "double"]
           |    }
           |  ]
           |}
           |""".stripMargin
    val avroSchema = new Schema.Parser().parse(avroSchemaString)
    val expectedSchema = StructType(Seq(
      StructField("complexCol", StructType(Seq(
        StructField(
          "member0",
          BooleanType,
          nullable = false,
          metadata = new MetadataBuilder()
            .putString(AvroTypeKey, "\"boolean\"")
            .build()
        ),
        StructField(
          "member1",
          IntegerType,
          nullable = false,
          metadata = new MetadataBuilder()
            .putString(AvroTypeKey, "\"int\"")
            .build()
        ),
        StructField(
          "member2",
          DoubleType,
          nullable = false,
          metadata = new MetadataBuilder()
            .putString(AvroTypeKey, "\"double\"")
            .build()
        )
      )))
    ))

    val schema = underTest.toSqlType(avroSchema)

    schema shouldBe expectedSchema
  }

  private def getAvroSchemaFromJson(resourcePath: String) = {
    val stream = getClass.getClassLoader.getResourceAsStream(resourcePath)
    val bufferedSource = Source.fromInputStream(stream)
    val schemaString = try bufferedSource.getLines().mkString finally bufferedSource.close()
    new Schema.Parser().parse(schemaString)
  }
}
