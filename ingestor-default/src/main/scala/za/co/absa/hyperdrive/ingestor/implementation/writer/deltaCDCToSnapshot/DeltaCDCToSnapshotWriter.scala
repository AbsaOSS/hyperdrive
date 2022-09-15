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

package za.co.absa.hyperdrive.ingestor.implementation.writer.deltaCDCToSnapshot

import org.apache.commons.configuration2.Configuration
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.{Column, DataFrame, Row, functions}
import org.slf4j.LoggerFactory
import za.co.absa.hyperdrive.compatibility.api.DeltaCDCToSnapshotWriterConfiguration
import za.co.absa.hyperdrive.compatibility.provider.CompatibleDeltaCDCToSnapshotWriterProvider
import za.co.absa.hyperdrive.ingestor.api.utils.{ConfigUtils, StreamWriterUtil}
import za.co.absa.hyperdrive.ingestor.api.writer.{StreamWriter, StreamWriterFactory, StreamWriterProperties}

private[writer] class DeltaCDCToSnapshotWriter(destination: String,
                                        trigger: Trigger,
                                        checkpointLocation: String,
                                        partitionColumns: Seq[String],
                                        keyColumn: String,
                                        opColumn: String,
                                        deletedValue: String,
                                        sortColumns: Seq[String],
                                        sortColumnsCustomOrder: Map[String, Seq[String]],
                                        val extraConfOptions: Map[String, String]) extends StreamWriter {
  if (StringUtils.isBlank(destination)) {
    throw new IllegalArgumentException(s"Invalid DELTA destination: '$destination'")
  }
  val compatibleDeltaCDCToSnapshotWriter = CompatibleDeltaCDCToSnapshotWriterProvider.provide(
    DeltaCDCToSnapshotWriterConfiguration(
      StreamWriterProperties.CheckpointLocation,
      destination,
      trigger,
      checkpointLocation,
      partitionColumns,
      keyColumn,
      opColumn,
      deletedValue,
      sortColumns,
      sortColumnsCustomOrder,
      extraConfOptions
  ))

  override def write(dataFrame: DataFrame): StreamingQuery = {
    compatibleDeltaCDCToSnapshotWriter.write(dataFrame)
  }
}

object DeltaCDCToSnapshotWriter extends StreamWriterFactory with DeltaCDCToSnapshotWriterAttributes {

  def apply(config: Configuration): StreamWriter = {
    val destinationDirectory = getDestinationDirectory(config)
    val trigger = StreamWriterUtil.getTrigger(config)
    val checkpointLocation = StreamWriterUtil.getCheckpointLocation(config)
    val extraOptions = getExtraOptions(config)
    val partitionColumns = ConfigUtils.getSeqOrNone(KEY_PARTITION_COLUMNS, config).getOrElse(Seq())

    val keyColumn = ConfigUtils.getOrThrow(KEY_KEY_COLUMN, config)
    val opColumn = ConfigUtils.getOrThrow(KEY_OP_COLUMN, config)
    val deletedValue = ConfigUtils.getOrThrow(KEY_OP_DELETED_VALUE, config)
    val sortColumns = ConfigUtils.getSeqOrThrow(KEY_SORT_COLUMNS, config)
    val sortColumnsCustomOrder =  ConfigUtils.getMapOrEmpty(KEY_SORT_COLUMNS_CUSTOM_ORDER, config)


    LoggerFactory.getLogger(this.getClass).info(s"Going to create DeltaStreamWriter instance using: " +
      s"destination directory='$destinationDirectory', trigger='$trigger', checkpointLocation='$checkpointLocation', extra options='$extraOptions'")

    new DeltaCDCToSnapshotWriter(destinationDirectory, trigger, checkpointLocation, partitionColumns, keyColumn, opColumn, deletedValue, sortColumns, sortColumnsCustomOrder, extraOptions)
  }

  def getDestinationDirectory(configuration: Configuration): String = ConfigUtils.getOrThrow(KEY_DESTINATION_DIRECTORY, configuration, errorMessage = s"Destination directory not found. Is '$KEY_DESTINATION_DIRECTORY' defined?")

  def getExtraOptions(configuration: Configuration): Map[String, String] = ConfigUtils.getPropertySubset(configuration, KEY_EXTRA_CONFS_ROOT)

  override def getExtraConfigurationPrefix: Option[String] = Some(KEY_EXTRA_CONFS_ROOT)
}
