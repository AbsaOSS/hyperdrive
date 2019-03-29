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
import za.co.absa.abris.avro.schemas.policy.SchemaRetentionPolicies
import za.co.absa.hyperdrive.transformer.encoding.schema.SchemaPathProvider

class TestAvroDecoder extends FlatSpec with MockitoSugar {

  behavior of "AvroDecoder"

  it should "throw on null SchemaPathProvider" in {
    assertThrows[IllegalArgumentException](new AvroDecoder(null, SchemaRetentionPolicies.RETAIN_SELECTED_COLUMN_ONLY))
  }

  it should "throw on null SchemaRetentionPolicy" in {
    val schemaPathProvider = mock[SchemaPathProvider]
    assertThrows[IllegalArgumentException](new AvroDecoder(schemaPathProvider, null))
  }

  it should "throw on null StreamDataReader" in {
    val schemaPathProvider = mock[SchemaPathProvider]
    val schemaRetentionPolicy = SchemaRetentionPolicies.RETAIN_SELECTED_COLUMN_ONLY
    val avroDecoder = new AvroDecoder(schemaPathProvider, schemaRetentionPolicy)
    assertThrows[IllegalArgumentException](avroDecoder.decode(streamReader = null))
  }
}
