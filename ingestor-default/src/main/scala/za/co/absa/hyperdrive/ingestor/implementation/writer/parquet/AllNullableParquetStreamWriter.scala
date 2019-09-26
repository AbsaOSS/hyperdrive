/*
 * Copyright 2018-2019 ABSA Group Limited
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

package za.co.absa.hyperdrive.ingestor.implementation.writer.parquet

import org.apache.commons.configuration2.Configuration
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery
import za.co.absa.hyperdrive.ingestor.api.manager.OffsetManager
import za.co.absa.hyperdrive.ingestor.api.writer.{StreamWriter, StreamWriterFactory}
import za.co.absa.hyperdrive.shared.utils.SparkUtils

/**
  * Works as a decorator for [[ParquetStreamWriter]] by setting the nullability of all fields of the
  * incoming DataFrame to null.
  */
private[parquet] class AllNullableParquetStreamWriter(val parquetStreamWriter: StreamWriter)
  extends StreamWriter(parquetStreamWriter.getDestination) {

  override def getDestination: String = parquetStreamWriter.getDestination

  override def write(dataFrame: DataFrame, offsetManager: OffsetManager): StreamingQuery = {
    val allNullableDataFrame = SparkUtils.setAllColumnsNullable(dataFrame)
    parquetStreamWriter.write(allNullableDataFrame, offsetManager)
  }
}

/**
  * Reuses factory for [[ParquetStreamWriter]].
  */
object AllNullableParquetStreamWriter extends StreamWriterFactory {

  override def apply(config: Configuration): StreamWriter = {
    LogManager.getLogger.info(s"Going to create ${classOf[AllNullableParquetStreamWriter].getSimpleName} instance. Injecting ${classOf[ParquetStreamWriter].getSimpleName} instance.")
    val parquetWriter = ParquetStreamWriter(config)
    new AllNullableParquetStreamWriter(parquetWriter)
  }
}
