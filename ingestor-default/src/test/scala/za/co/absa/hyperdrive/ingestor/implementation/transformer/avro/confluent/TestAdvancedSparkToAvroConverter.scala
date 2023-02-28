
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
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.io.Source

class TestAdvancedSparkToAvroConverter extends AnyFlatSpec with Matchers {
  it should "create the same avro schema for non-nullable types as the default implementation" in {
    val schema = StructType(Seq(
      StructField("booleanCol", BooleanType, nullable = false),
      StructField("byteCol", ByteType, nullable = false),
      StructField("shortCol", ShortType, nullable = false),
      StructField("intCol", IntegerType, nullable = false),
      StructField("longCol", LongType, nullable = false),
      StructField("dateCol", DateType, nullable = false),
      StructField("floatCol", FloatType, nullable = false),
      StructField("doubleCol", DoubleType, nullable = false),
      StructField("stringCol", StringType, nullable = false),
      StructField("timestampCol", TimestampType, nullable = false),
      StructField("decimalCol", new DecimalType(10, 3), nullable = false),
      StructField("binaryCol", BinaryType, nullable = false),
      StructField("arrayCol", ArrayType(StringType, containsNull = false), nullable = false),
      StructField("mapCol", MapType(StringType, StringType, valueContainsNull = false), nullable = false),
      StructField("nestedCol", StructType(Seq(StructField("stringCol", StringType, nullable = false))), nullable = false)
    ))

    val expectedSchema = SchemaConverters.toAvroType(schema, nullable = false, "recordName", "nameSpace")
    val avroSchema = AdvancedSparkToAvroConverter(schema, nullable = false, "recordName", "nameSpace")

    avroSchema shouldBe expectedSchema
  }

  it should "create a union schema with nulls first for nullable types" in {
    val schema = StructType(Seq(
      StructField("booleanCol", BooleanType),
      StructField("byteCol", ByteType),
      StructField("shortCol", ShortType),
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

    val expectedSchema = getAvroSchemaFromJson("avro-spark-conversion/nullable-types-nulls-first.json")
    val avroSchema = AdvancedSparkToAvroConverter(schema, nullable = false, "recordName", "nameSpace")

    avroSchema shouldBe expectedSchema
  }

  it should "add default values to the avro schema if specified in metadata" in {
    val schema = StructType(Seq(
      StructField("stringCol", StringType,
        metadata = new MetadataBuilder().putString(SparkMetadataKeys.DefaultValueKey, "\"abcd\"").build()),
      StructField("intCol", IntegerType,
        metadata = new MetadataBuilder().putString(SparkMetadataKeys.DefaultValueKey, "null").build()),
      StructField("arrayCol", ArrayType(StringType),
        metadata = new MetadataBuilder().putString(SparkMetadataKeys.DefaultValueKey, "[\"value1\", \"value2\"]").build()),
      StructField("arrayNonNullCol", ArrayType(StringType, containsNull = false),
        metadata = new MetadataBuilder().putString(SparkMetadataKeys.DefaultValueKey, "[\"value1\", \"value2\"]").build()),
      StructField("mapCol", MapType(StringType, StringType),
        metadata = new MetadataBuilder().putString(SparkMetadataKeys.DefaultValueKey, "{\"value1\": \"value2\"}").build()),
      StructField("mapNonNullCol", MapType(StringType, StringType, valueContainsNull = false),
        metadata = new MetadataBuilder().putString(SparkMetadataKeys.DefaultValueKey, "{\"value1\": \"value2\"}").build()),
      StructField("nestedCol", StructType(Seq(
        StructField(
          "stringCol",
          StringType,
          metadata = new MetadataBuilder().putString(SparkMetadataKeys.DefaultValueKey, "\"xyz\"").build()
        ))))
    ))

    val expectedSchema = getAvroSchemaFromJson("avro-spark-conversion/types-with-defaults.json")
    val avroSchema = AdvancedSparkToAvroConverter(schema)

    println(avroSchema.toString(true))

    avroSchema shouldBe expectedSchema
  }

  it should "create the correct logical avro type for timestamp type based on metadata" in {
    val schema = StructType(Seq(
      StructField("timestampMillis", TimestampType,
        metadata = new MetadataBuilder()
          .putString(SparkMetadataKeys.AvroTypeKey, raw"""{"type":"long","logicalType":"timestamp-millis"}""")
          .build()),
      StructField("timestampMicros", TimestampType,
        metadata = new MetadataBuilder()
          .putString(SparkMetadataKeys.AvroTypeKey, raw"""{"type":"long","logicalType":"timestamp-micros"}""")
          .build()),
      StructField("timestampMicrosNoMetadata", TimestampType)
    ))

    val expectedSchema = getAvroSchemaFromJson("avro-spark-conversion/timestamp-types.json")
    val avroSchema = AdvancedSparkToAvroConverter(schema)

    avroSchema shouldBe expectedSchema
  }

  it should "create the correct avro type for decimal type based on metadata" in {
    val decimalType = new DecimalType(8, 3)
    val decimalFixedSchema =
      raw"""{
           |"type": "fixed",
           |"name": "fixed",
           |"namespace": "any-namespace",
           |"size": 10,
           |"logicalType": "decimal",
           |"precision": 23,
           |"scale": 3}""".stripMargin
    val binaryFixedSchema =
      raw"""{
           |"type": "fixed",
           |"name": "fixed",
           |"namespace": "any-namespace",
           |"size": 10}""".stripMargin
    val schema = StructType(Seq(
      StructField("decimalBytes", decimalType, nullable = false,
        metadata = new MetadataBuilder()
          .putString(SparkMetadataKeys.AvroTypeKey, raw"""{"type":"bytes","logicalType":"decimal"}""")
          .build()),
      StructField("decimalFixed", decimalType, nullable = false,
        metadata = new MetadataBuilder()
          .putString(SparkMetadataKeys.AvroTypeKey, decimalFixedSchema)
          .build()),
      StructField("decimalNoMetadata", decimalType, nullable = false),
      StructField("binaryBytes", BinaryType, nullable = false,
        metadata = new MetadataBuilder()
          .putString(SparkMetadataKeys.AvroTypeKey, raw"""{"type":"bytes"}""")
          .build()),
      StructField("binaryFixed", BinaryType, nullable = false,
        metadata = new MetadataBuilder()
          .putString(SparkMetadataKeys.AvroTypeKey, binaryFixedSchema)
          .build()),
      StructField("binaryNoMetadata", BinaryType, nullable = false)
    ))

    val expectedSchema = getAvroSchemaFromJson("avro-spark-conversion/decimal-types.json")
    val avroSchema = AdvancedSparkToAvroConverter(schema)

    avroSchema shouldBe expectedSchema
  }

  private def getAvroSchemaFromJson(resourcePath: String) = {
    val stream = getClass.getClassLoader.getResourceAsStream(resourcePath)
    val bufferedSource = Source.fromInputStream(stream)
    val schemaString = try bufferedSource.getLines().mkString finally bufferedSource.close()
    new Schema.Parser().parse(schemaString)
  }
}
