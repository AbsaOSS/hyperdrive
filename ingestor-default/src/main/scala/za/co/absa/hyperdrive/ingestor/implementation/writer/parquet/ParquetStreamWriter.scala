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

package za.co.absa.hyperdrive.ingestor.implementation.writer.parquet

import org.apache.commons.configuration2.Configuration
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.streaming.Trigger
import za.co.absa.hyperdrive.ingestor.api.utils.{ConfigUtils, StreamWriterUtil}
import za.co.absa.hyperdrive.ingestor.api.writer.{StreamWriter, StreamWriterFactory}
import za.co.absa.hyperdrive.ingestor.implementation.writer.parquet.AbstractParquetStreamWriter._
import za.co.absa.hyperdrive.shared.configurations.ConfigurationsKeys.ParquetStreamWriterKeys
import za.co.absa.hyperdrive.shared.configurations.ConfigurationsKeys.ParquetStreamWriterKeys.KEY_EXTRA_CONFS_ROOT

private[writer] class ParquetStreamWriter(destination: String, trigger: Trigger,
                                          partitionColumns: Option[Seq[String]],
                                          doMetadataCheck: Boolean,
                                          extraConfOptions: Map[String, String])
  extends AbstractParquetStreamWriter(destination, trigger, partitionColumns, doMetadataCheck, extraConfOptions)

object ParquetStreamWriter extends StreamWriterFactory with ParquetStreamWriterAttributes {

  def apply(config: Configuration): StreamWriter = {
    val destinationDirectory = getDestinationDirectory(config)
    val doMetadataCheck = getMetadataCheck(config)
    val trigger = StreamWriterUtil.getTrigger(config)
    val partitionColumns = ConfigUtils.getSeqOrNone(ParquetStreamWriterKeys.KEY_PARTITION_COLUMNS, config)
    val extraOptions = getExtraOptions(config)

    LogManager.getLogger.info(s"Going to create ParquetStreamWriter instance using: " +
      s"destination directory='$destinationDirectory', trigger='$trigger', extra options='$extraOptions'")

    new ParquetStreamWriter(destinationDirectory, trigger, partitionColumns, doMetadataCheck, extraOptions)
  }

  override def getExtraConfigurationPrefix: Option[String] = Some(KEY_EXTRA_CONFS_ROOT)
}
