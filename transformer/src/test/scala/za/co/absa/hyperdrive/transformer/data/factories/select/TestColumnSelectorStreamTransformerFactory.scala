/*
 *  Copyright 2019 ABSA Group Limited
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package za.co.absa.hyperdrive.transformer.data.factories.select

import org.apache.commons.configuration2.DynamicCombinedConfiguration
import org.apache.commons.configuration2.BaseConfiguration
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler
import org.scalatest.mockito.MockitoSugar
import org.scalatest.FlatSpec
import za.co.absa.hyperdrive.transformer.data.impl.select.ColumnSelectorStreamTransformer
import za.co.absa.hyperdrive.shared.configurations.ConfigurationsKeys.ColumnSelectorStreamTransformerKeys._

class TestColumnSelectorStreamTransformerFactory extends FlatSpec with MockitoSugar {

  behavior of ColumnSelectorStreamTransformerFactory.getClass.getSimpleName

  it should "create ColumnSelectorStreamTransformer for columns specified in configurations" in {
    val columns = Seq("a","b","c")
    val config = new DynamicCombinedConfiguration()
    config.setListDelimiterHandler(new DefaultListDelimiterHandler(','))
    config.addProperty(KEY_COLUMNS_TO_SELECT, columns.mkString(","))

    val transformer = ColumnSelectorStreamTransformerFactory.build(config).asInstanceOf[ColumnSelectorStreamTransformer]
    assert(columns == transformer.columns)
  }

  it should "assume all columns are should be selected if none is specified in configurations" in {
    val config = new BaseConfiguration()

    val transformer = ColumnSelectorStreamTransformerFactory.build(config).asInstanceOf[ColumnSelectorStreamTransformer]
    assert(Seq("*") == transformer.columns)
  }
}
