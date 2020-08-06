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

import java.util.concurrent.CountDownLatch

import org.apache.commons.configuration2.Configuration
import org.apache.commons.lang3.StringUtils
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}
import za.co.absa.hyperdrive.ingestor.api.reader.{StreamReader, StreamReaderFactory}
import za.co.absa.hyperdrive.ingestor.api.utils.{ComponentFactoryUtil, ConfigUtils}
import za.co.absa.hyperdrive.shared.utils.FileUtils

import scala.util.{Failure, Success, Try}

/**
 * Creates a parquet reader for a file source.
 *
 * @param path       Path to the file source.
 * @param extraConfs Extra configurations, see Spark documentation
 */
private[reader] class ParquetStreamReader(
  val path: String,
  val waitForFiles: Boolean,
  val checkForFilesInterval: Long,
  val extraConfs: Map[String, String]) extends StreamReader {

  private val logger = LogManager.getLogger()

  private[reader] def waitingForFilesHookForTesting(): Unit = {
    // do nothing
  }

  override def read(spark: SparkSession): DataFrame = {

    if (spark.sparkContext.isStopped) {
      throw new IllegalStateException("SparkSession is stopped.")
    }

    logger.info(s"Will read from path $path")

    if(waitForFiles) {
      val fs = spark.sparkContext.hadoopConfiguration
      while (FileUtils.dirContainsNoParquetFilesOrDoesNotExist(path, fs)) {
        waitingForFilesHookForTesting()
        Thread.sleep(checkForFilesInterval)
      }
    }

    val schema = spark.read.load(path).schema

    spark
      .readStream
      .schema(schema)
      .options(extraConfs)
      .parquet(path)
  }
}

object ParquetStreamReader extends StreamReaderFactory with ParquetStreamReaderAttributes {
  private val logger = LogManager.getLogger
  private val DefaultCheckForInitialFileInterval = 10000L
  private val DefaultWaitForFiles = false

  override def apply(conf: Configuration): StreamReader = {
    ComponentFactoryUtil.validateConfiguration(conf, getProperties)

    val path = conf.getString(KeySourceDirectory)
    val waitForFiles = ConfigUtils.getBooleanOrNone(KeyWaitForFiles, conf).getOrElse(DefaultWaitForFiles)
    val checkForInitialFileInterval = conf.getLong(KeyCheckForInitialFileInterval, DefaultCheckForInitialFileInterval)
    if (waitForFiles && checkForInitialFileInterval < 0) {
      throw new IllegalArgumentException(s"${KeyCheckForInitialFileInterval} cannot be negative")
    }

    val extraOptions = ConfigUtils.getPropertySubset(conf, getExtraConfigurationPrefix.get)

    logger.info(s"Going to create ParquetStreamReader with: path=$path, waitForFiles=$waitForFiles, " +
      s"checkForInitialFileInterval=$checkForInitialFileInterval, extraOptions=$extraOptions")

    new ParquetStreamReader(path, waitForFiles, checkForInitialFileInterval, extraOptions)
  }
}
