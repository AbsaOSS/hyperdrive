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

import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler
import org.apache.commons.configuration2.{BaseConfiguration, Configuration}
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


  "getSeqOrNone" should "return the value as an array if key exists" in {
    // given
    val config = new BaseConfiguration()
    config.setListDelimiterHandler(new DefaultListDelimiterHandler(','))
    config.addProperty("key", "value, value2, value3, value4")

    // when
    val value = ConfigUtils.getSeqOrNone("key", config)

    // then
    value.get should contain theSameElementsAs Seq("value", "value2", "value3", "value4")
  }

  it should "return None if array is empty" in {
    // given
    val config = new BaseConfiguration()
    config.addProperty("key", "")

    // when
    val value = ConfigUtils.getSeqOrNone("key", config)

    // then
    value shouldBe None
  }

  it should "return None if key doesn't exist" in {
    // given
    val config = new BaseConfiguration()

    // when
    val value = ConfigUtils.getSeqOrNone("non-existent", config)

    // then
    value shouldBe None
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

  "copyAndMapConfig" should "add the specified properties to the target config" in {
    // given
    val sourceConfig = new BaseConfiguration
    sourceConfig.addProperty("key1", "sourceValue1")
    sourceConfig.addProperty("key2", 2)
    sourceConfig.addProperty("key3", 3)

    val targetConfig = new BaseConfiguration
    targetConfig.addProperty("key1", "targetValue1")

    val mapping = Map("key1" -> "source.key1", "key3" -> "key3")

    // when
    val result = ConfigUtils.copyAndMapConfig(sourceConfig, targetConfig, mapping)

    // then
    result.isSuccess shouldBe true
    import scala.collection.JavaConverters._
    result.get.getKeys.asScala.toList should contain theSameElementsAs Seq("key1", "source.key1", "key3")
    result.get.getString("key1") shouldBe "targetValue1"
    result.get.getString("source.key1") shouldBe "sourceValue1"
    result.get.getInt("key3") shouldBe 3
  }

  it should "not change the target config if the mapping is empty" in {
    val sourceConfig = new BaseConfiguration
    val targetConfig = new BaseConfiguration
    targetConfig.addProperty("key1", "targetValue1")
    targetConfig.addProperty("key2", 2)
    targetConfig.addProperty("key3", 3)

    val result = ConfigUtils.copyAndMapConfig(sourceConfig, targetConfig, Map())

    result.isSuccess shouldBe true
    import scala.collection.JavaConverters._
    result.get.getKeys.asScala.toList should contain theSameElementsAs Seq("key1", "key2", "key3")
    result.get.getString("key1") shouldBe "targetValue1"
    result.get.getInt("key2") shouldBe 2
    result.get.getInt("key3") shouldBe 3
  }

  it should "return a failure if source keys don't exist" in {
    // given
    val sourceConfig = new BaseConfiguration
    val targetConfig = new BaseConfiguration

    val mapping = Map("key1" -> "", "key3" -> "key3")

    // when
    val result = ConfigUtils.copyAndMapConfig(sourceConfig, targetConfig, mapping)

    // then
    result.isFailure shouldBe true
    result.failed.get.getMessage should include ("key1, key3")
  }

  it should "return a failure if target keys already exist" in {
    // given
    val sourceConfig = new BaseConfiguration
    sourceConfig.addProperty("key1", "sourceValue1")
    sourceConfig.addProperty("key2", 2)
    sourceConfig.addProperty("key3", 3)

    val targetConfig = new BaseConfiguration
    targetConfig.addProperty("key2", "some value")
    targetConfig.addProperty("key3", "some value")

    val mapping = Map("key1" -> "", "key2" -> "key2", "key3" -> "key3")

    // when
    val result = ConfigUtils.copyAndMapConfig(sourceConfig, targetConfig, mapping)

    // then
    result.isFailure shouldBe true
    result.failed.get.getMessage should include ("key2, key3")
  }

  "getBooleanOrNone" should "return true if property is empty" in {
    // given
    val config = new BaseConfiguration
    config.addProperty("key1", "")

    // when
    val result = ConfigUtils.getBooleanOrNone("key1", config)

    // then
    result.get shouldBe true
  }

  it should "return true if property is true or True" in {
    // given
    val config = new BaseConfiguration
    config.addProperty("key1", "true")
    config.addProperty("key2", "True")

    // when
    val result1 = ConfigUtils.getBooleanOrNone("key1", config)
    val result2 = ConfigUtils.getBooleanOrNone("key2", config)

    // then
    result1.get shouldBe true
    result2.get shouldBe true
  }

  it should "return false if property is set to false" in {
    // given
    val config = new BaseConfiguration
    config.addProperty("key1", "false")
    config.addProperty("key2", "False")

    // when
    val result1 = ConfigUtils.getBooleanOrNone("key1", config)
    val result2 = ConfigUtils.getBooleanOrNone("key2", config)

    // then
    result1.get shouldBe false
    result2.get shouldBe false
  }

  it should "return a failure if property is not boolean" in {
    // given
    val config = new BaseConfiguration
    config.addProperty("key1", "pravda")
    config.addProperty("key2", "1")
    config.addProperty("key3", "0")

    // when
    val result1 = intercept[Exception](ConfigUtils.getBooleanOrNone("key1", config))
    val result2 = intercept[Exception](ConfigUtils.getBooleanOrNone("key2", config))
    val result3 = intercept[Exception](ConfigUtils.getBooleanOrNone("key3", config))

    // then
    result1.getMessage should include("key1")
    result2.getMessage should include("key2")
    result3.getMessage should include("key3")
  }

  it should "return None if property is absent" in {
    // given
    val config = new BaseConfiguration

    // when
    val result = ConfigUtils.getBooleanOrNone("key1", config)

    // then
    result shouldBe None
  }
}
