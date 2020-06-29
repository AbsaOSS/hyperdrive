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

package za.co.absa.hyperdrive.ingestor.implementation.transformer.dateversion

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.commons.configuration2.Configuration
import org.apache.hadoop.fs.Path
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.execution.streaming.{FileStreamSink, MetadataLogFileIndex}
import org.apache.spark.sql.functions.{lit, to_date}
import org.apache.spark.sql.{DataFrame, SparkSession}
import za.co.absa.hyperdrive.ingestor.api.transformer.{StreamTransformer, StreamTransformerFactory}
import za.co.absa.hyperdrive.ingestor.api.utils.ConfigUtils.getOrThrow

private[transformer] class AddDateVersionTransformer(val reportDate: String, val destination: String) extends StreamTransformer {

  import AddDateVersionTransformer.{ColumnDate, ColumnVersion}
  override def transform(dataFrame: DataFrame): DataFrame = {
    val spark = dataFrame.sparkSession
    val initialVersion = 1
    val nextVersion = findNextVersion(spark, initialVersion)
    val dfWithDate = dataFrame
      .withColumn(ColumnDate, to_date(lit(reportDate), AddDateVersionTransformer.reportDateFormat))
      .withColumn(ColumnVersion, lit(nextVersion))
    dfWithDate
  }

  private def findNextVersion(spark: SparkSession, initialVersion: Int): Int = {
    if (noCommittedParquetFilesExist(spark)) {
      initialVersion
    } else {
      import spark.implicits._
      val df = spark.read.parquet(destination)
      val versions = df.select(df(ColumnVersion))
        .filter(df(ColumnDate) === lit(reportDate))
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

object AddDateVersionTransformer extends StreamTransformerFactory with AddDateVersionTransformerAttributes {
  val reportDateFormat: String = "yyyy-MM-dd"
  val reportDateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(reportDateFormat)
  val ColumnDate = "hyperdrive_date"
  val ColumnVersion = "hyperdrive_version"

  def apply(config: Configuration): StreamTransformer = {
    val reportDate = getReportDateString(config)
    val destinationDirectory = getDestinationDirectory(config)

    LogManager.getLogger.info(s"Going to create AddDateVersionTransformer instance")

    new AddDateVersionTransformer(reportDate, destinationDirectory)
  }

  private def getReportDateString(configuration: Configuration): String = {
    configuration.getString(KeyReportDate) match {
      case value: String => value
      case _ => reportDateFormatter.format(LocalDate.now())
    }
  }

  def getDestinationDirectory(configuration: Configuration): String =
    getOrThrow(KeyDestinationDirectory, configuration, errorMessage = s"Destination directory not found. Is '$KeyDestinationDirectory' defined?")

}
