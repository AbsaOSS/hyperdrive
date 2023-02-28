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

package za.co.absa.hyperdrive.ingestor.implementation.transformer.column.copy

import org.apache.commons.configuration2.DynamicCombinedConfiguration
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class TestColumnCopyStreamTransformerObject extends AnyFlatSpec with Matchers {
  behavior of ColumnCopyStreamTransformer.getClass.getSimpleName

  it should "create ColumnCopyStreamTransformer for column pairs specified in configurations" in {
    val columnsFrom = Seq("a", "b", "c")
    val columnsTo = Seq("A", "b2", "C3")
    val config = new DynamicCombinedConfiguration()
    config.setListDelimiterHandler(new DefaultListDelimiterHandler(','))
    config.addProperty(ColumnCopyStreamTransformer.KEY_COLUMNS_FROM, columnsFrom.mkString(","))
    config.addProperty(ColumnCopyStreamTransformer.KEY_COLUMNS_TO, columnsTo.mkString(","))

    val transformer = ColumnCopyStreamTransformer(config).asInstanceOf[ColumnCopyStreamTransformer]
    transformer.columnsFrom should contain theSameElementsAs columnsFrom
    transformer.columnsTo should contain theSameElementsAs columnsTo
  }

  it should "throw an exception if columns are empty" in {
    val config = new DynamicCombinedConfiguration()
    config.setListDelimiterHandler(new DefaultListDelimiterHandler(','))
    config.addProperty(ColumnCopyStreamTransformer.KEY_COLUMNS_FROM, "")
    config.addProperty(ColumnCopyStreamTransformer.KEY_COLUMNS_TO, "")

    val ex = the[Exception] thrownBy ColumnCopyStreamTransformer(config)
    ex.getMessage should include(ColumnCopyStreamTransformer.KEY_COLUMNS_FROM)
  }

  it should "throw an exception if columns from are not specified"  in {
    val config = new DynamicCombinedConfiguration()
    config.setListDelimiterHandler(new DefaultListDelimiterHandler(','))
    config.addProperty(ColumnCopyStreamTransformer.KEY_COLUMNS_TO, "a,b,c")

    val exception = the[Exception] thrownBy ColumnCopyStreamTransformer(config)
    exception.getMessage should include(ColumnCopyStreamTransformer.KEY_COLUMNS_FROM)
  }

  it should "throw an exception if columns to are not specified"  in {
    val config = new DynamicCombinedConfiguration()
    config.setListDelimiterHandler(new DefaultListDelimiterHandler(','))
    config.addProperty(ColumnCopyStreamTransformer.KEY_COLUMNS_FROM, "a,b,c")

    val exception = the[Exception] thrownBy ColumnCopyStreamTransformer(config)
    exception.getMessage should include(ColumnCopyStreamTransformer.KEY_COLUMNS_TO)
  }
}
