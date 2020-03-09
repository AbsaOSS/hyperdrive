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

package za.co.absa.hyperdrive.shared.utils

import org.apache.commons.configuration2.{BaseConfiguration, Configuration}
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

class TestConfigUtils extends FlatSpec with Matchers with MockitoSugar {

  behavior of s"ConfigUtils"

  "getOrThrow" should "return the value if key exists" in {
    // given
    val config = new BaseConfiguration()
    config.addProperty("key", "value")

    // when
    val value = ConfigUtils.getOrThrow("key", config)

    // then
    value shouldBe "value"
  }

  "getOrThrow" should "throw an exception if key doesn't exist" in {
    // given
    val config = new BaseConfiguration()

    // when
    val throwable = intercept[IllegalArgumentException](ConfigUtils.getOrThrow("non-existent", config, "some-error-message"))

    // then
    throwable.getMessage shouldBe "some-error-message"
  }

  "getOrThrow" should "throw an exception if key doesn't exist with default error message" in {
    // given
    val config = new BaseConfiguration()

    // when
    val throwable = intercept[IllegalArgumentException](ConfigUtils.getOrThrow("non-existent", config))

    // then
    throwable.getMessage shouldBe "No configuration property found for key non-existent"
  }

  "getSeqOrThrow" should "return the value as an array if key exists" in {
    // given
    val config = new BaseConfiguration()
    config.setListDelimiterHandler(new DefaultListDelimiterHandler(','))
    config.addProperty("key", "value, value2, value3, value4")

    // when
    val value = ConfigUtils.getSeqOrThrow("key", config)

    // then
    value should contain theSameElementsAs Seq("value", "value2", "value3", "value4")
  }

  "getSeqOrThrow" should "throw an exception if array is empty" in {
    // given
    val config = new BaseConfiguration()
    config.addProperty("key", "")

    // when
    val throwable = intercept[IllegalArgumentException](ConfigUtils.getSeqOrThrow("key", config, "some-error-message"))

    // then
    throwable.getMessage shouldBe "some-error-message"
  }

  "getSeqOrThrow" should "throw an exception if key doesn't exist" in {
    // given
    val config = new BaseConfiguration()

    // when
    val throwable = intercept[IllegalArgumentException](ConfigUtils.getSeqOrThrow("non-existent", config, "some-error-message"))

    // then
    throwable.getMessage shouldBe "some-error-message"
  }

  "getSeqOrThrow" should "throw an exception if key doesn't exist with default error message" in {
    // given
    val config = new BaseConfiguration()

    // when
    val throwable = intercept[IllegalArgumentException](ConfigUtils.getSeqOrThrow("non-existent", config))

    // then
    throwable.getMessage shouldBe "No configuration property found for key non-existent"
  }

  "getOrNone" should "return the value as an option if key exists" in {
    // given
    val config = new BaseConfiguration()
    config.addProperty("key", "value")

    // when
    val value = ConfigUtils.getOrNone("key", config)

    // then
    value shouldBe Some("value")
  }

  "getOrNone" should "return None if key doesn't exist" in {
    // given
    val config = new BaseConfiguration()

    // when
    val value = ConfigUtils.getOrNone("non-existent", config)

    // then
    value shouldBe None
  }

  "getPropertySubset" should "return only properties beginning with a prefix" in {
    // given
    val config = new BaseConfiguration()
    config.addProperty("some.prefix.some.category.key1", "value1")
    config.addProperty("some.prefix.some.category.key2", "value2")
    config.addProperty("some.prefix.-key-three", "value3")
    config.addProperty("other.prefix.some.category.key1", "value1")

    // when
    val properties = ConfigUtils.getPropertySubset(config, "some.prefix")

    // then
    properties should contain theSameElementsAs Map(
      "some.category.key1" -> "value1",
      "some.category.key2" -> "value2",
      "-key-three" -> "value3")
  }

  "getPropertySubset" should "return an empty map if no prefixed properties exist" in {
    // given
    val config = new BaseConfiguration()

    // when
    val properties = ConfigUtils.getPropertySubset(config, "some.prefix")

    // then
    properties shouldBe empty
  }

  "getPropertySubset" should "return an empty map if Configuration.subset returns null" in {
    val config = mock[Configuration]

    val properties = ConfigUtils.getPropertySubset(config, "")

    properties shouldBe empty
  }
}
