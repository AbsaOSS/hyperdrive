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

package za.co.absa.hyperdrive.ingestor.implementation.reader.parquet

import org.apache.commons.configuration2.Configuration
import org.slf4j.LoggerFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import za.co.absa.hyperdrive.ingestor.api.reader.{StreamReader, StreamReaderFactory}
import za.co.absa.hyperdrive.ingestor.api.utils.{ComponentFactoryUtil, ConfigUtils}

/**
 * Creates a parquet reader for a file source.
 *
 * @param path       Path to the file source.
 * @param extraConfs Extra configurations, see Spark documentation
 */
private[reader] class ParquetStreamReader(
  val path: String,
  val extraConfs: Map[String, String]) extends StreamReader {

  private val logger = LoggerFactory.getLogger(this.getClass)()

  override def read(spark: SparkSession): DataFrame = {

    if (spark.sparkContext.isStopped) {
      throw new IllegalStateException("SparkSession is stopped.")
    }

    logger.info(s"Will read from path $path")

    val schema = spark.read.load(path).schema

    spark
      .readStream
      .schema(schema)
      .options(extraConfs)
      .parquet(path)
  }
}

object ParquetStreamReader extends StreamReaderFactory with ParquetStreamReaderAttributes {
  private val logger = LoggerFactory.getLogger(this.getClass)

  override def apply(conf: Configuration): StreamReader = {
    ComponentFactoryUtil.validateConfiguration(conf, getProperties)

    val path = conf.getString(KeySourceDirectory)

    val extraOptions = ConfigUtils.getPropertySubset(conf, getExtraConfigurationPrefix.get)

    logger.info(s"Going to create ParquetStreamReader with: path=$path, extraOptions=$extraOptions")

    new ParquetStreamReader(path, extraOptions)
  }
}
