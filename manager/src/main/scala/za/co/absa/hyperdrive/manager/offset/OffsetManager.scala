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

package za.co.absa.hyperdrive.manager.offset

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter}

/**
  * Base class for all OffsetManagers.
  *
  * The process for adding a new OffsetManager implementation is:
  * <ul>
  * <li>1. Create the implementation package (e.g. za.co.absa.hyperdrive.manager.offset.impl.jdbc).</li>
  * <li>2. Add the implementation (e.g. za.co.absa.hyperdrive.manager.offset.impl.jdbc.JDBCOffsetManager).</li>
  * <li>3. Create the factory package (e.g. za.co.absa.hyperdrive.manager.offset.factories.jdbc).</li>
  * <li>4. Add the factory as an implementation of [[za.co.absa.hyperdrive.manager.offset.OffsetManager]].</li>
  * <li>5. Add the factory to the abstract offset manager factory at [[za.co.absa.hyperdrive.manager.offset.factories.OffsetManagerAbstractFactory]] as described in the class documentation.</li>
  * </ul>
  */
abstract class OffsetManager(topic: String) {

  def configureOffsets(streamReader: DataStreamReader, configuration: Configuration): DataStreamReader

  def configureOffsets(streamWriter: DataStreamWriter[Row], configuration: Configuration): DataStreamWriter[Row]
}
