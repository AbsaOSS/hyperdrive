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

package za.co.absa.hyperdrive.ingestor.implementation

import za.co.absa.hyperdrive.ingestor.implementation.transformer.column.selection.ColumnSelectorStreamTransformer
import za.co.absa.hyperdrive.ingestor.implementation.transformer.factories.StreamTransformerAbstractFactory
import za.co.absa.hyperdrive.shared.configurations.ConfigurationsKeys.ColumnSelectorStreamTransformerKeys

/**
 * This class provides default values for configuration keys which are necessary to run the ingestor.
 * However, it is not sufficient to rely on the default configuration only, because for some properties
 * no meaningful defaults exist.
 */
private[hyperdrive] object DefaultConfiguration {
  val values: Map[String, String] = Map(
    StreamTransformerAbstractFactory.componentConfigKey -> ColumnSelectorStreamTransformer.getClass.getName,
    ColumnSelectorStreamTransformerKeys.KEY_COLUMNS_TO_SELECT -> "*"
  )
}
