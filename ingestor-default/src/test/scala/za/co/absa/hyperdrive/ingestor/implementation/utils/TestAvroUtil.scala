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
import org.apache.avro.generic.{GenericData, GenericRecord, GenericRecordBuilder}
import org.apache.avro.util.Utf8
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TestAvroUtil extends AnyFlatSpec with Matchers with BeforeAndAfter {

  private val valueSchemaString = raw"""
      {"type": "record", "name": "schemaName", "fields": [
      {"type": "int", "name": "record_id"},
      {"type": ["null", "schemaName"], "name": "child_record", "nullable": true}
      ]}"""

  private val keySchemaString = raw"""
      {"type": "record", "name": "keySchema", "fields": [
      {"type": "string", "name": "key"}
      ]}"""

  "getIdColumnsFromRecord" should "get the specified fields from the record" in {
    // given
    val parser = new Parser()
    val valueSchema = parser.parse(valueSchemaString)
    val childRecord2 = new GenericData.Record(valueSchema)
    childRecord2.put("record_id", 13)
    childRecord2.put("child_record", null)
    val childRecord1 = new GenericData.Record(valueSchema)
    childRecord1.put("record_id", 12)
    childRecord1.put("child_record", childRecord2)
    val valueRecord = new GenericData.Record(valueSchema)
    valueRecord.put("record_id", 11)
    valueRecord.put("child_record", childRecord1)

    val keySchema = parser.parse(keySchemaString)
    val keyRecord = new GenericData.Record(keySchema)
    keyRecord.put("key", new Utf8("abcdef"))

    val consumerRecord: ConsumerRecord[GenericRecord, GenericRecord] =
      new ConsumerRecord("topicName", 0, 42, keyRecord, valueRecord)

    // when, then
    AvroUtil.getFromConsumerRecord(consumerRecord, "topic") shouldBe Some("topicName")
    AvroUtil.getFromConsumerRecord(consumerRecord, "offset") shouldBe Some(42)
    AvroUtil.getFromConsumerRecord(consumerRecord, "partition") shouldBe Some(0)
    AvroUtil.getFromConsumerRecord(consumerRecord, "key.key") shouldBe Some("abcdef")
    AvroUtil.getFromConsumerRecord(consumerRecord, "value.record_id") shouldBe Some(11)
    AvroUtil.getFromConsumerRecord(consumerRecord, "value.child_record.record_id") shouldBe Some(12)
    AvroUtil.getFromConsumerRecord(consumerRecord, "value.child_record.child_record.record_id") shouldBe Some(13)
  }

  it should "return None if a record is not nested as expected" in {
    // given
    val parser = new Parser()
    val valueSchema = parser.parse(valueSchemaString)
    val valueRecord = new GenericData.Record(valueSchema)
    valueRecord.put("record_id", 11)
    valueRecord.put("child_record", null)

    val consumerRecord: ConsumerRecord[GenericRecord, GenericRecord] =
      new ConsumerRecord("topicName", 0, 42, null, valueRecord)

    // when, then
    AvroUtil.getFromConsumerRecord(consumerRecord, "value.child_record.child_record.record_id") shouldBe None
  }

  it should "return None if a field does not exist" in {
    // given
    val parser = new Parser()
    val valueSchema = parser.parse(valueSchemaString)
    val valueRecord = new GenericData.Record(valueSchema)
    valueRecord.put("record_id", 11)
    valueRecord.put("child_record", null)

    val keySchema = parser.parse(keySchemaString)
    val keyRecord = new GenericData.Record(keySchema)
    keyRecord.put("key", new Utf8("abcdef"))

    val consumerRecord: ConsumerRecord[GenericRecord, GenericRecord] =
      new ConsumerRecord("topicName", 0, 42, keyRecord, valueRecord)

    // when, then
    AvroUtil.getFromConsumerRecord(consumerRecord, "key.some_nonexistent_field") shouldBe None
  }

  it should "return None if a field does not exist on the consumer record" in {
    // given
    val consumerRecord: ConsumerRecord[GenericRecord, GenericRecord] =
      new ConsumerRecord("topicName", 0, 42, null, null)

    // when, then
    AvroUtil.getFromConsumerRecord(consumerRecord, "some_nonexistent_field") shouldBe None
  }

  it should "return None if a field on the record is requested, but the record is null" in {
    // given
    val consumerRecord: ConsumerRecord[GenericRecord, GenericRecord] =
      new ConsumerRecord("topicName", 0, 42, null, null)

    // when, then
    AvroUtil.getFromConsumerRecord(consumerRecord, "key.some_nonexistent_field") shouldBe None
  }
}
