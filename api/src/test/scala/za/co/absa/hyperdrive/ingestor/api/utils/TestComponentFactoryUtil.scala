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

package za.co.absa.hyperdrive.ingestor.api.utils

import org.apache.commons.configuration2.BaseConfiguration
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.hyperdrive.ingestor.api.PropertyMetadata

class TestComponentFactoryUtil extends AnyFlatSpec with Matchers {
  behavior of ComponentFactoryUtil.getClass.getName

  "validateConfiguration" should "not throw an error if the configuration is valid" in {
    val properties = Map(
      "some.key.1" -> PropertyMetadata("label", None, required = true),
      "some.key.2" -> PropertyMetadata("label", None, required = true),
      "some.key.3" -> PropertyMetadata("label", None, required = false)
    )
    val config = new BaseConfiguration
    config.addProperty("some.key.1", "any value")
    config.addProperty("some.key.2", "any value")


    ComponentFactoryUtil.validateConfiguration(config, properties)
  }

  it should "throw an error if the configuration is not valid" in {
    val properties = Map(
      "some.key.1" -> PropertyMetadata("label", None, required = true),
      "some.key.2" -> PropertyMetadata("label", None, required = true),
      "some.key.3" -> PropertyMetadata("label", None, required = false)
    )
    val config = new BaseConfiguration
    config.addProperty("some.key.1", "any value")
    config.addProperty("some.key.3", "any value")


    val throwable = intercept[IllegalArgumentException](ComponentFactoryUtil.validateConfiguration(config, properties))
    throwable.getMessage should include ("some.key.2")
  }

}
