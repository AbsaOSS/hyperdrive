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

import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericData, GenericRecordBuilder}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class TestAvroUtil extends FlatSpec with Matchers with BeforeAndAfter {
  "getRecursively" should "get recursively" in {
    // given
    val rootSchemaString = raw"""
      {"type": "record", "name": "schemaName", "fields": [
      {"type": "int", "name": "record_id"},
      {"type": ["null", "schemaName"], "name": "child_record", "nullable": true}
      ]}"""

    val parser = new Parser()
    val schema = parser.parse(rootSchemaString)

    val childRecord2 = new GenericData.Record(schema)
    childRecord2.put("record_id", 3)
    childRecord2.put("child_record", null)
    val childRecord1 = new GenericData.Record(schema)
    childRecord1.put("record_id", 2)
    childRecord1.put("child_record", childRecord2)
    val rootRecord = new GenericData.Record(schema)
    rootRecord.put("record_id", 1)
    rootRecord.put("child_record", childRecord1)

    // when, then
    AvroUtil.getRecursively(rootRecord, List("record_id")).asInstanceOf[Int] shouldBe 1
    AvroUtil.getRecursively(rootRecord, List("child_record", "record_id")).asInstanceOf[Int] shouldBe 2
    AvroUtil.getRecursively(rootRecord, List("child_record", "child_record", "record_id")).asInstanceOf[Int] shouldBe 3
  }

}
