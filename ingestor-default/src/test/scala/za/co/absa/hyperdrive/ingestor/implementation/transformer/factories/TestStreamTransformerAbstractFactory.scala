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

package za.co.absa.hyperdrive.ingestor.implementation.transformer.factories

import org.apache.commons.configuration2.BaseConfiguration
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import za.co.absa.hyperdrive.ingestor.implementation.transformer.column.selection.ColumnSelectorStreamTransformer
import za.co.absa.hyperdrive.ingestor.implementation.transformer.factories.DummyStreamTransformer._
import za.co.absa.hyperdrive.ingestor.api.transformer.StreamTransformerFactory._

class TestStreamTransformerAbstractFactory extends FlatSpec with BeforeAndAfterEach with MockitoSugar with Matchers {

  behavior of StreamTransformerAbstractFactory.getClass.getSimpleName

  it should "create transformer instances in the correct order" in {
    val config = getBaseConfiguration
    config.addProperty(s"${IdsKeyPrefix}.1", "dummy.transformer.A")
    config.addProperty(s"${ClassKeyPrefix}.dummy.transformer.A", DummyStreamTransformer.getClass.getName)
    config.addProperty(s"${TransformerKeyPrefix}.dummy.transformer.A.$DummyProperty1Name", "value1")
    config.addProperty(s"${TransformerKeyPrefix}.dummy.transformer.A.$DummyProperty2Name", "100")

    config.addProperty(s"${IdsKeyPrefix}.2", "dummy.transformer.B")
    config.addProperty(s"${ClassKeyPrefix}.dummy.transformer.B", DummyStreamTransformer.getClass.getName)
    config.addProperty(s"${TransformerKeyPrefix}.dummy.transformer.B.$DummyProperty1Name", "value2")
    config.addProperty(s"${TransformerKeyPrefix}.dummy.transformer.B.$DummyProperty2Name", "200")

    val transformers = StreamTransformerAbstractFactory.build(config)
    transformers should have size 2
    all(transformers) shouldBe a[DummyStreamTransformer]

    val firstTransformer = transformers.head.asInstanceOf[DummyStreamTransformer]
    firstTransformer.dummyProperty1 shouldBe "value1"
    firstTransformer.dummyProperty2 shouldBe 100
    firstTransformer.dummyProperty3 shouldBe "global.value.1"
    firstTransformer.dummyProperty4 shouldBe "global.value.2"
    val secondTransformer = transformers(1).asInstanceOf[DummyStreamTransformer]
    secondTransformer.dummyProperty1 shouldBe "value2"
    secondTransformer.dummyProperty2 shouldBe 200
    firstTransformer.dummyProperty3 shouldBe "global.value.1"
    firstTransformer.dummyProperty4 shouldBe "global.value.2"
  }

  it should "support negative orders" in {
    import StreamTransformerAbstractFactory._
    val config = getBaseConfiguration
    config.addProperty(s"${IdsKeyPrefix}.2", "[column.transformer]")
    config.addProperty(s"${ClassKeyPrefix}.[column.transformer]", ColumnSelectorStreamTransformer.getClass.getName)

    config.addProperty(s"${IdsKeyPrefix}.-1", "dummy.transformer.A")
    config.addProperty(s"${ClassKeyPrefix}.dummy.transformer.A", DummyStreamTransformer.getClass.getName)

    val transformers = StreamTransformerAbstractFactory.build(config)
    transformers should have size 2
    transformers.head shouldBe a[DummyStreamTransformer]
    transformers(1) shouldBe a[ColumnSelectorStreamTransformer]
  }

  it should "return an empty list if no configuration is given" in {
    val config = getBaseConfiguration
    val transformers = StreamTransformerAbstractFactory.build(config)
    transformers shouldBe empty
  }

  it should "throw if transformer ids are not unique" in {
    val config = getBaseConfiguration
    config.addProperty(s"${IdsKeyPrefix}.1", "dummy.transformer.A")
    config.addProperty(s"${IdsKeyPrefix}.2", "dummy.transformer.A")

    val throwable = intercept[IllegalArgumentException](StreamTransformerAbstractFactory.build(config))
    throwable.getMessage should include(s"dummy.transformer.A")
  }

  it should "throw if transformer id is non-numeric" in {
    val config = getBaseConfiguration
    config.addProperty(s"${IdsKeyPrefix}.First", "dummy.transformer.A")

    val throwable = intercept[IllegalArgumentException](StreamTransformerAbstractFactory.build(config))
    throwable.getMessage should include(s"${IdsKeyPrefix}.First")
  }

  it should "throw if no class name is associated to the transformer id" in {
    val config = getBaseConfiguration
    config.addProperty(s"${IdsKeyPrefix}.1", "dummy.transformer.A")

    val throwable = intercept[IllegalArgumentException](StreamTransformerAbstractFactory.build(config))
    throwable.getMessage should include(s"${ClassKeyPrefix}.dummy.transformer.A")
  }

  it should "throw if data transformer parameter is invalid" in {
    val invalidFactoryName = "an-invalid-factory-name"
    val config = getBaseConfiguration
    config.addProperty(s"${IdsKeyPrefix}.1", "dummy.transformer.A")
    config.addProperty(s"${ClassKeyPrefix}.dummy.transformer.A", invalidFactoryName)
    val throwable = intercept[IllegalArgumentException](StreamTransformerAbstractFactory.build(config))

    assert(throwable.getMessage.contains(invalidFactoryName))
  }

  private def getBaseConfiguration = {
    val config = new BaseConfiguration
    config.addProperty(GlobalConfigKeys.GlobalKey1, "global.value.1")
    config.addProperty(GlobalConfigKeys.GlobalKey2, "global.value.2")
    config.addProperty(GlobalConfigKeys.GlobalKey3, "global.value.3")
    config
  }

}
