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

package za.co.absa.hyperdrive.ingestor.implementation.transformer.enceladus.columns

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.commons.configuration2.Configuration
import org.slf4j.LoggerFactory
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{lit, to_date, typedLit}
import za.co.absa.hyperdrive.ingestor.api.transformer.{StreamTransformer, StreamTransformerFactory}

private[transformer] class AddEnceladusColumnsTransformer(val reportDate: String) extends StreamTransformer {

  import AddEnceladusColumnsTransformer._
  override def transform(dataFrame: DataFrame): DataFrame = {
    val version = 1
    val dfWithDate = dataFrame
      .withColumn(ErrorColumn, typedLit(List[ErrorMessage]()))
      .withColumn(InfoDateColumn, to_date(lit(reportDate), AddEnceladusColumnsTransformer.reportDateFormat))
      .withColumn(InfoDateStringColumn, lit(reportDate))
      .withColumn(InfoVersionColumn, lit(version))
    dfWithDate
  }
}

object AddEnceladusColumnsTransformer extends StreamTransformerFactory with AddEnceladusColumnsTransformerAttributes {
  val reportDateFormat: String = "yyyy-MM-dd"
  val reportDateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(reportDateFormat)
  val InfoDateStringColumn = "enceladus_info_date_string"
  val InfoDateColumn = "enceladus_info_date"
  val InfoVersionColumn = "enceladus_info_version"
  val ErrorColumn = "errCol"

  def apply(config: Configuration): StreamTransformer = {
    val reportDate = getReportDateString(config)

    LoggerFactory.getLogger(this.getClass).info(s"Going to create AddEnceladusColumnsTransformer instance")

    new AddEnceladusColumnsTransformer(reportDate)
  }

  private def getReportDateString(configuration: Configuration): String = {
    configuration.getString(KeyReportDate) match {
      case value: String => value
      case _ => reportDateFormatter.format(LocalDate.now())
    }
  }
}
