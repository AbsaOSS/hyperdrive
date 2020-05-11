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

package za.co.absa.hyperdrive.driver.drivers

import java.io.File

import org.apache.commons.configuration2.{Configuration, PropertiesConfiguration}
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder
import org.apache.commons.configuration2.builder.fluent.Parameters
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler
import org.apache.logging.log4j.LogManager
import za.co.absa.hyperdrive.driver.IngestionDriver
import za.co.absa.hyperdrive.driver.utils.DriverUtil

/**
  * This driver launches ingestion by loading the configurations from a properties file.
  */
object PropertiesIngestionDriver extends IngestionDriver {

  private val logger = LogManager.getLogger

  def main(args: Array[String]): Unit = {
    val propertiesFile = getPropertiesFilePath(args)
    if (propertiesFile.isEmpty) {
      throw new IllegalArgumentException("No properties file supplied.")
    }
    logger.info(s"Starting Hyperdrive ${DriverUtil.getVersionString}")

    if (isInvalid(propertiesFile.get)) {
      throw new IllegalArgumentException(s"Invalid properties file: '${propertiesFile.get}'.")
    }

    logger.info(s"Going to load ingestion configurations from '${propertiesFile.get}'.")
    val configurations = loadConfiguration(propertiesFile.get)
    logger.info(s"Configurations loaded. Going to invoke ingestion: [$configurations]")
    ingest(configurations)
  }

  def loadConfiguration(path: String): Configuration = {
    val parameters = new Parameters()
    new FileBasedConfigurationBuilder[PropertiesConfiguration](classOf[PropertiesConfiguration])
      .configure(parameters.fileBased()
        .setListDelimiterHandler(new DefaultListDelimiterHandler(ListDelimiter))
        .setFile(new File(path)))
      .getConfiguration
  }

  private def getPropertiesFilePath(args: Array[String]): Option[String] = {
    args.length match {
      case v if v == 0 => None
      case v =>
        if (v > 1) {
          logger.warn(s"Expected only properties file path, but got extra parameters. Returning first as the path. All parameters = [${args.mkString(",")}]")
        }
        Some(args(0))
    }
  }

  private def isInvalid(path: String): Boolean = {
    val file = new File(path)
    !file.exists() || !file.isFile
  }
}
