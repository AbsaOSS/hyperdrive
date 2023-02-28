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

package za.co.absa.hyperdrive.ingestor.implementation.transformer.column.renaming

import org.apache.commons.configuration2.{BaseConfiguration, DynamicCombinedConfiguration}
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler
import ColumnRenamingStreamTransformer._
import org.scalatest.flatspec.AnyFlatSpec

class TestColumnRenamingStreamTransformerObject extends AnyFlatSpec {
  behavior of ColumnRenamingStreamTransformer.getClass.getSimpleName

  it should "create ColumnRenamingStreamTransformer for column pairs specified in configurations" in {
    val columnsFrom = Seq("a", "b", "c")
    val columnsTo = Seq("A", "b2", "C3")
    val config = new DynamicCombinedConfiguration()
    config.setListDelimiterHandler(new DefaultListDelimiterHandler(','))
    config.addProperty(KEY_COLUMNS_FROM, columnsFrom.mkString(","))
    config.addProperty(KEY_COLUMNS_TO, columnsTo.mkString(","))

    val transformer = ColumnRenamingStreamTransformer(config).asInstanceOf[ColumnRenamingStreamTransformer]
    assert(columnsFrom == transformer.columnsFrom)
    assert(columnsTo == transformer.columnsTo)
  }

  it should "throw an exception if source columns are not specified" in {
    val columnsTo = Seq("A", "b2", "C3")
    val config = new DynamicCombinedConfiguration()
    config.setListDelimiterHandler(new DefaultListDelimiterHandler(','))
    config.addProperty(KEY_COLUMNS_TO, columnsTo.mkString(","))

    val ex = intercept[IllegalArgumentException] {
      ColumnRenamingStreamTransformer(config).asInstanceOf[ColumnRenamingStreamTransformer]
    }

    assert(ex.getMessage.contains("Empty list of columns to rename."))
  }

  it should "throw an exception if target columns are not specified" in {
    val columnsFrom = Seq("a", "b", "c")
    val config = new DynamicCombinedConfiguration()
    config.setListDelimiterHandler(new DefaultListDelimiterHandler(','))
    config.addProperty(KEY_COLUMNS_FROM, columnsFrom.mkString(","))

    val ex = intercept[IllegalArgumentException] {
      ColumnRenamingStreamTransformer(config).asInstanceOf[ColumnRenamingStreamTransformer]
    }

    assert(ex.getMessage.contains("Empty list of columns to rename."))
  }

  it should "throw an exception if two lists do not match" in {
    val columnsFrom = Seq("a", "b", "c")
    val columnsTo = Seq("A", "b2")
    val config = new DynamicCombinedConfiguration()
    config.setListDelimiterHandler(new DefaultListDelimiterHandler(','))
    config.addProperty(KEY_COLUMNS_FROM, columnsFrom.mkString(","))
    config.addProperty(KEY_COLUMNS_TO, columnsTo.mkString(","))

    val ex = intercept[IllegalArgumentException] {
      ColumnRenamingStreamTransformer(config).asInstanceOf[ColumnRenamingStreamTransformer]
    }

    assert(ex.getMessage.contains("The size of source column names doesn't match"))
  }
}
