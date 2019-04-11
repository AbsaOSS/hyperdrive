/*
 *
 * Copyright 2019 ABSA Group Limited
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package za.co.absa.hyperdrive.transformer.encoding.impl

import org.scalatest.FlatSpec
import org.scalatest.mockito.MockitoSugar
import za.co.absa.abris.avro.read.confluent.SchemaManager
import za.co.absa.abris.avro.schemas.policy.SchemaRetentionPolicies
import za.co.absa.hyperdrive.shared.InfrastructureSettings.SchemaRegistrySettings

class TestAvroStreamDecoder extends FlatSpec with MockitoSugar {

  private lazy val SCHEMA_REGISTRY_ACCESS_SETTINGS = Map(
    SchemaManager.PARAM_SCHEMA_REGISTRY_URL          -> "http://localhost:8081",
    SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> SchemaRegistrySettings.VALUE_SCHEMA_NAMING_STRATEGY,
    SchemaManager.PARAM_KEY_SCHEMA_NAMING_STRATEGY   -> SchemaRegistrySettings.VALUE_SCHEMA_NAMING_STRATEGY
  )

  behavior of "AvroDecoder"

  it should "throw on blank topic" in {
    assertThrows[IllegalArgumentException](new AvroStreamDecoder(topic = null, SCHEMA_REGISTRY_ACCESS_SETTINGS, SchemaRetentionPolicies.RETAIN_SELECTED_COLUMN_ONLY))
    assertThrows[IllegalArgumentException](new AvroStreamDecoder(topic = "  ", SCHEMA_REGISTRY_ACCESS_SETTINGS, SchemaRetentionPolicies.RETAIN_SELECTED_COLUMN_ONLY))
  }

  it should "throw on null Schema Registry settings" in {
    assertThrows[IllegalArgumentException](new AvroStreamDecoder(topic = "topic", schemaRegistrySettings = null, SchemaRetentionPolicies.RETAIN_SELECTED_COLUMN_ONLY))
  }
  it should "throw on empty Schema Registry settings" in {
    assertThrows[IllegalArgumentException](new AvroStreamDecoder(topic = "topic", schemaRegistrySettings = Map[String,String](), SchemaRetentionPolicies.RETAIN_SELECTED_COLUMN_ONLY))
  }

  it should "throw on null SchemaRetentionPolicy" in {
    assertThrows[IllegalArgumentException](new AvroStreamDecoder(topic = "topic", SCHEMA_REGISTRY_ACCESS_SETTINGS, retentionPolicy = null))
  }

  it should "throw on null StreamDataReader" in {
    val schemaRetentionPolicy = SchemaRetentionPolicies.RETAIN_SELECTED_COLUMN_ONLY

    val avroDecoder = new AvroStreamDecoder(topic = "topic", SCHEMA_REGISTRY_ACCESS_SETTINGS, schemaRetentionPolicy)
    assertThrows[IllegalArgumentException](avroDecoder.decode(streamReader = null))
  }
}
