
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

import org.apache.avro.Schema
import org.apache.spark.sql.avro.SchemaConverters
import org.scalatest.{FlatSpec, Matchers}

import scala.io.{BufferedSource, Source}

class TestSchemaPreservation extends FlatSpec with Matchers {
  private def getSchemaString(name: String, namespace: String) = {
    raw"""{
     "type": "record",
     "name": "$name",
     "namespace": "$namespace",
     "fields":[
         {"name": "nullableInt", "type": ["int", "null"], "customField": "hello" },
         {"name": "turnover", "default": null, "type": ["null", {"logicalType": "decimal", "precision": 23, "scale": 3, "type": "bytes"}]}
     ]
    }"""
  }

  it should "not die" in {
    val stream = getClass.getClassLoader.getResourceAsStream("test-schema-logical-type-default.json")
//    val stream = getClass.getClassLoader.getResourceAsStream("test-schema-nullable.json")
    val bufferedSource = Source.fromInputStream(stream)
    val schema = try bufferedSource.getLines().mkString finally bufferedSource.close()
    val avroSchema = new Schema.Parser().parse(schema)
    val catalystSchema = CustomSchemaConverters.toSqlType(avroSchema)
    val avroSchema2 = CustomSchemaConverters.toAvroType(catalystSchema, None, false, None,"name", "topLevelNamespace")

    println(avroSchema2.toString(true))
    avroSchema2 shouldBe avroSchema
  }

  it should "still not die" in {
    val avroSchema = new Schema.Parser().parse(getSchemaString("name", "topLevelNamespace"))
    val props = avroSchema.getObjectProps
    val schemaString = avroSchema.toString(false)
    val avroSchema2 = new Schema.Parser().parse(schemaString)
    avroSchema2 shouldBe avroSchema
  }


}
