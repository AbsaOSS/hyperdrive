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

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.commons.configuration2.Configuration
import org.apache.hadoop.fs.Path
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.execution.streaming.{FileStreamSink, MetadataLogFileIndex}
import org.apache.spark.sql.functions.{lit, to_date}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}
import za.co.absa.hyperdrive.ingestor.api.utils.StreamWriterUtil
import za.co.absa.hyperdrive.ingestor.api.writer.{StreamWriter, StreamWriterFactory}
import za.co.absa.hyperdrive.ingestor.implementation.writer.parquet.AbstractParquetStreamWriter._
import za.co.absa.hyperdrive.shared.configurations.ConfigurationsKeys.ParquetPartitioningStreamWriterKeys._
import za.co.absa.hyperdrive.shared.configurations.ConfigurationsKeys.ParquetStreamWriterKeys.KEY_EXTRA_CONFS_ROOT


private[writer] class ParquetPartitioningStreamWriter(destination: String, trigger: Trigger,
                                                      partitionColumns: Option[Seq[String]],
                                                      reportDate: String, extraConfOptions: Map[String, String])
  extends AbstractParquetStreamWriter(destination, trigger, partitionColumns, extraConfOptions) {

  import ParquetPartitioningStreamWriter.{COL_DATE, COL_VERSION}
  override protected def transformDataframe(dataFrame: DataFrame): DataFrame = {
    val spark = dataFrame.sparkSession
    val initialVersion = 1
    val nextVersion = findNextVersion(spark, initialVersion)
    val dfWithDate = dataFrame
      .withColumn(COL_DATE, to_date(lit(reportDate), ParquetPartitioningStreamWriter.reportDateFormat))
      .withColumn(COL_VERSION, lit(nextVersion))
    dfWithDate
  }

  private def findNextVersion(spark: SparkSession, initialVersion: Int): Int = {
    if (noCommittedParquetFilesExist(spark)) {
      initialVersion
    } else {
      import spark.implicits._
      val df = spark.read.parquet(destination)
      val versions = df.select(df(COL_VERSION))
        .filter(df(COL_DATE) === lit(reportDate))
        .distinct()
        .as[Int]
        .collect().toList

      if (versions.nonEmpty) versions.max + 1 else initialVersion
    }
  }

  private def noCommittedParquetFilesExist(spark: SparkSession): Boolean = {
    val fileCatalog = new MetadataLogFileIndex(spark, new Path(destination), None)
    !FileStreamSink.hasMetadata(Seq(destination), spark.sparkContext.hadoopConfiguration) || fileCatalog.allFiles().isEmpty
  }
}

object ParquetPartitioningStreamWriter extends StreamWriterFactory with ParquetPartitioningStreamWriterAttributes {
  val reportDateFormat: String = "yyyy-MM-dd"
  val reportDateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(reportDateFormat)
  val COL_DATE = "hyperdrive_date"
  val COL_VERSION = "hyperdrive_version"

  def apply(config: Configuration): StreamWriter = {
    val destinationDirectory = getDestinationDirectory(config)
    val trigger = StreamWriterUtil.getTrigger(config)
    val partitionColumns = Some(Seq(COL_DATE, COL_VERSION))
    val reportDateString = getReportDateString(config)
    val extraOptions = getExtraOptions(config)

    LogManager.getLogger.info(s"Going to create ParquetPartitioningStreamWriter instance using: " +
      s"destination directory='$destinationDirectory', trigger='$trigger', extra options='$extraOptions'")

    new ParquetPartitioningStreamWriter(destinationDirectory, trigger, partitionColumns, reportDateString, extraOptions)
  }

  private def getReportDateString(configuration: Configuration): String = {
    configuration.getString(KEY_REPORT_DATE) match {
      case value: String => value
      case _ => reportDateFormatter.format(LocalDate.now())
    }
  }

  override def getExtraConfigurationPrefix: Option[String] = Some(KEY_EXTRA_CONFS_ROOT)
}
