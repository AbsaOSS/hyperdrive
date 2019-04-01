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

package za.co.absa.hyperdrive.writer.impl

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import za.co.absa.hyperdrive.manager.offset.OffsetManager
import za.co.absa.hyperdrive.writer.StreamWriter

class ParquetStreamWriter(destination: String) extends StreamWriter(destination) {

  if (StringUtils.isBlank(destination)) {
    throw new IllegalArgumentException(s"Invalid PARQUET destination: '$destination'")
  }

  def write(dataFrame: DataFrame, offsetManager: OffsetManager): StreamingQuery = {
    if (dataFrame == null) {
      throw new IllegalArgumentException("Null DataFrame.")
    }

    if (offsetManager == null) {
      throw new IllegalArgumentException("Null OffsetManager instance.")
    }

    val outStream = dataFrame
      .writeStream
      .trigger(Trigger.Once())
      .format(source = "parquet")
      .outputMode(OutputMode.Append())

    offsetManager.configureOffsets(outStream)
      .start(destination)
  }
}
