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

import org.scalatest.{FlatSpec, Matchers}
import za.co.absa.abris.avro.read.confluent.SchemaManager.SchemaStorageNamingStrategies._

class TestSchemaRegistrySettingsUtil extends FlatSpec with Matchers {

  behavior of SchemaRegistrySettingsUtil.getClass.getName

  "namingStrategyInvolvesRecord" should "return true only for record strategies" in {
    SchemaRegistrySettingsUtil.namingStrategyInvolvesRecord(RECORD_NAME) shouldBe true
    SchemaRegistrySettingsUtil.namingStrategyInvolvesRecord(TOPIC_RECORD_NAME) shouldBe true
    SchemaRegistrySettingsUtil.namingStrategyInvolvesRecord(TOPIC_NAME) shouldBe false
    SchemaRegistrySettingsUtil.namingStrategyInvolvesRecord("anything") shouldBe false
  }
}
