/*
 *
 * Copyright 2019 ABSA Group Limited
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package za.co.absa.hyperdrive.transformer.data

import org.apache.spark.sql.DataFrame

/**
  * Base class for all StreamTransformers.
  *
  * The process for adding a new StreamTransformer implementation is:
  * <ul>
  * <li>1. Create the implementation package (e.g. za.co.absa.hyperdrive.transformer.data.impl.enceladus).</li>
  * <li>2. Add the implementation (e.g. za.co.absa.hyperdrive.transformer.data.impl.enceladus.EnceladusStreamTransformer).</li>
  * <li>3. Create the factory package (e.g. za.co.absa.hyperdrive.transformer.data.factories.enceladus).</li>
  * <li>4. Add the factory as an implementation of [[za.co.absa.hyperdrive.transformer.data.StreamTransformer]].</li>
  * <li>5. Add the factory to the abstract stream transformer factory at [[za.co.absa.hyperdrive.transformer.data.factories.StreamTransformerAbstractFactory]] as described in the class documentation.</li>
  * </ul>
  */
abstract class StreamTransformer {
  def transform(streamData: DataFrame): DataFrame
}
