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
import org.apache.commons.configuration2.{BaseConfiguration, Configuration, ConfigurationConverter}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import za.co.absa.hyperdrive.ingestor.api.transformer.StreamTransformerFactory

class TestConfigUtils extends AnyFlatSpec with Matchers with MockitoSugar {

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
    result.failed.get.getMessage should include("key1, key3")
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
    result.failed.get.getMessage should include("key2, key3")
  }

  "getBoolean" should "return a boolean if the value is convertible to a boolean" in {
    val config = new BaseConfiguration
    config.addProperty("key1", "true")
    config.addProperty("key2", "True")
    config.addProperty("key3", "yes")
    config.addProperty("key4", true)
    config.addProperty("key5", "false")
    config.addProperty("key6", "FALSE")
    config.addProperty("key7", "no")
    config.addProperty("key8", false)

    ConfigUtils.getOptionalBoolean("key1", config).get shouldBe true
    ConfigUtils.getOptionalBoolean("key2", config).get shouldBe true
    ConfigUtils.getOptionalBoolean("key3", config).get shouldBe true
    ConfigUtils.getOptionalBoolean("key4", config).get shouldBe true
    ConfigUtils.getOptionalBoolean("key5", config).get shouldBe false
    ConfigUtils.getOptionalBoolean("key6", config).get shouldBe false
    ConfigUtils.getOptionalBoolean("key7", config).get shouldBe false
    ConfigUtils.getOptionalBoolean("key8", config).get shouldBe false
  }

  it should "return None if the key doesn't exist" in {
    val config = new BaseConfiguration

    ConfigUtils.getOptionalBoolean("any-key", config) shouldBe None
  }

  it should "throw an exception if the value is empty or not a boolean" in {
    val config = new BaseConfiguration
    config.addProperty("key1", "")
    config.addProperty("key2", "nay")
    config.addProperty("key4", 0)

    val ex1 = the[Exception] thrownBy ConfigUtils.getOptionalBoolean("key1", config)
    ex1.getMessage should include("key1")
    val ex2 = the[Exception] thrownBy ConfigUtils.getOptionalBoolean("key2", config)
    ex2.getMessage should include("key2")
    val ex4 = the[Exception] thrownBy ConfigUtils.getOptionalBoolean("key4", config)
    ex4.getMessage should include("key4")
  }

  "getMapOrEmpty" should "return the value as an map if key exists" in {
    // given
    val config = new BaseConfiguration()
    config.setListDelimiterHandler(new DefaultListDelimiterHandler(','))
    config.addProperty("key.subKey", "value1, value2, value3, value4")

    // when
    val value = ConfigUtils.getMapOrEmpty("key", config)

    // then
    value should contain theSameElementsAs Map("subKey" -> List("value1", "value2", "value3", "value4"))
  }

  it should "return map with empty sequence if key exists but value is empty" in {
    // given
    val config = new BaseConfiguration()
    config.addProperty("key.subKey", "")

    // when
    val value = ConfigUtils.getMapOrEmpty("key", config)

    // then
    value should contain theSameElementsAs Map("subKey" -> List())
  }

  it should "return Empty map if key does not exist" in {
    // given
    val config = new BaseConfiguration()

    // when
    val value = ConfigUtils.getMapOrEmpty("key.subKey", config)

    // then
    value shouldBe Map.empty
  }

  "getTransformerPrefix" should "get the prefix of a transformer class" in {
    val config = new BaseConfiguration
    config.addProperty(s"${StreamTransformerFactory.ClassKeyPrefix}.[dummy-transformer]", classOf[DummyStreamTransformer].getCanonicalName)

    val prefix = ConfigUtils.getTransformerPrefix(config, classOf[DummyStreamTransformer])

    prefix shouldBe Some("[dummy-transformer]")
  }

  it should "return None if the transformer class is not registered in the config" in {
    val config = new BaseConfiguration

    val prefix = ConfigUtils.getTransformerPrefix(config, classOf[DummyStreamTransformer])

    prefix shouldBe None
  }

  "filterConfigKeys" should "filter out config keys" in {
    val map = Map(
      "some.sensitive.key" -> "sensitive value",
      "other.sensitive.key" -> "other sensitive value",
      "normal.key" -> "normal value"
    )

    val filteredMap = ConfigUtils.filterKeysContaining(map, "sensitive")

    filteredMap.size shouldBe 1
    filteredMap("normal.key") shouldBe "normal value"
  }

  "getSubsets" should "return a map of children keys to subset configs" in {
    val config = new BaseConfiguration
    config.addProperty("service.items.myItem1.name", "My Item 1")
    config.addProperty("service.items.myItem1.description", "Very nice item 1")
    config.addProperty("service.items.myItem2.name", "My Item 2")
    config.addProperty("service.items.myItem2.description", "Very nice item 2")
    config.addProperty("service.items.myItem2.options.color", "blue")
    config.addProperty("service.items.myItem2.options.size", "large")
    config.addProperty("service.items.myItem3", "The third item")
    config.addProperty("service.name", "Item Service")

    val subsets = ConfigUtils.getSubsets(config, "service.items")

    import scala.collection.JavaConverters._
    subsets.keys should contain theSameElementsAs Array("myItem1", "myItem2")
    ConfigurationConverter.getMap(subsets("myItem1")).asScala should contain theSameElementsAs Map(
      "name" -> "My Item 1",
      "description" -> "Very nice item 1"
    )
    ConfigurationConverter.getMap(subsets("myItem2")).asScala should contain theSameElementsAs Map(
      "name" -> "My Item 2",
      "description" -> "Very nice item 2",
      "options.color" -> "blue",
      "options.size" -> "large"
    )
  }

  "getSubsets" should "return the correct subsets if the prefix ends with a dot" in {
    val config = new BaseConfiguration
    config.addProperty("parent.child.grandchild.someStuff.value", "Anything")

    val subsets = ConfigUtils.getSubsets(config, "parent.child.grandchild.")

    import scala.collection.JavaConverters._
    subsets.keys should contain theSameElementsAs Array("someStuff")
    subsets("someStuff").getKeys.asScala.toSeq should contain theSameElementsAs Array("value")
    subsets("someStuff").getString("value") shouldBe "Anything"
  }

  "getSubsets" should "return an empty map if the prefix does not exist" in {
    val config = new BaseConfiguration
    config.addProperty("parent.child.grandchild.someStuff.value", "Anything")

    val subsets = ConfigUtils.getSubsets(config, "child.grandchild")

    subsets shouldBe Map()
  }
}
