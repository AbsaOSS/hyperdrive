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

import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder
import org.apache.commons.configuration2.builder.fluent.Parameters
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler
import org.apache.commons.configuration2.{Configuration, PropertiesConfiguration}
import org.slf4j.LoggerFactory
import za.co.absa.hyperdrive.driver.IngestionDriver

import java.nio.file.{Files, Paths}

/**
  * This driver launches ingestion by loading the configurations from a properties file.
  */
object PropertiesIngestionDriver extends IngestionDriver {
  private val logger = LoggerFactory.getLogger(this.getClass)

  override def loadConfiguration(args: Array[String]): Configuration = {
    val propertiesFile = getPropertiesFilePath(args)
    if (propertiesFile.isEmpty) {
      throw new IllegalArgumentException("No properties file supplied.")
    }

    if (isInvalid(propertiesFile.get)) {
      throw new IllegalArgumentException(s"Invalid properties file: '${propertiesFile.get}'.")
    }

    logger.info(s"Going to load ingestion configurations from '${propertiesFile.get}'.")
    loadConfigurationFromFile(propertiesFile.get)
  }

  private def loadConfigurationFromFile(path: String): Configuration = {
    val parameters = new Parameters()
    new FileBasedConfigurationBuilder[PropertiesConfiguration](classOf[PropertiesConfiguration])
      .configure(parameters.fileBased()
        .setListDelimiterHandler(new DefaultListDelimiterHandler(ListDelimiter))
        .setPath(path))
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

  private def isInvalid(pathStr: String): Boolean = {
    val path = Paths.get(pathStr)
    !Files.exists(path) || !Files.isReadable(path)
  }
}
