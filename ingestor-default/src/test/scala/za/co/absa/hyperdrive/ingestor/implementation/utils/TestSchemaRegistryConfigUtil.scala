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

import org.apache.commons.configuration2.BaseConfiguration
import org.scalatest.{FlatSpec, Matchers}
import za.co.absa.hyperdrive.ingestor.implementation.transformer.avro.confluent.SchemaRegistryAttributes

class TestSchemaRegistryConfigUtil extends FlatSpec with Matchers {

  "getSchemaRegistryConfig" should "extract the schema registry config in a map" in {
    // given
    val config = new BaseConfiguration
    config.addProperty(SchemaRegistryAttributes.KEY_SCHEMA_REGISTRY_URL, "http://localhost:8081")
    config.addProperty(s"${SchemaRegistryAttributes.KEY_SCHEMA_REGISTRY_EXTRA_CONFS_ROOT}.basic.auth.credentials.source", "USER_INFO")
    config.addProperty(s"${SchemaRegistryAttributes.KEY_SCHEMA_REGISTRY_EXTRA_CONFS_ROOT}.some-other-key", "some-value")

    // when
    val schemaRegistryConfig = SchemaRegistryConfigUtil.getSchemaRegistryConfig(config)

    // then
    schemaRegistryConfig("schema.registry.url") shouldBe "http://localhost:8081"
    schemaRegistryConfig("basic.auth.credentials.source") shouldBe "USER_INFO"
    schemaRegistryConfig("some-other-key") shouldBe "some-value"
  }

  it should "throw an exception if schema.registry.url is not specified" in {
    // given
    val config = new BaseConfiguration

    // when
    val exception = the[Exception] thrownBy SchemaRegistryConfigUtil.getSchemaRegistryConfig(config)

    // then
    exception.getMessage should include (SchemaRegistryAttributes.KEY_SCHEMA_REGISTRY_URL)

  }

}
