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

package za.co.absa.hyperdrive.driver.drivers

import org.apache.commons.configuration2.Configuration
import org.apache.commons.configuration2.BaseConfiguration
import org.apache.logging.log4j.LogManager
import za.co.absa.hyperdrive.driver.IngestionDriver

object CommandLineIngestionDriver extends IngestionDriver {

  private val logger = LogManager.getLogger

  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      throw new IllegalArgumentException("No configuration provided.")
    }

    logger.info(s"Going to load ${args.length} configurations from command line.")
    val configuration = parseConfiguration(args)
    logger.info("Configuration loaded. Going to invoke ingestion.")
    ingest(configuration)
  }

  def parseConfiguration(settings: Array[String]): Configuration = {
    val configuration = new BaseConfiguration
    settings.foreach(setOrThrow(_, configuration))
    configuration
  }

  private def setOrThrow(setting: String, configuration: Configuration): Unit = {
    val settingKeyValue = setting.trim.split("=", 2)
    if (settingKeyValue.length != 2 || settingKeyValue.exists(_.isEmpty)) {
      throw new IllegalArgumentException(s"Invalid setting format: $setting")
    }
    configuration.setProperty(settingKeyValue(0), settingKeyValue(1))
  }
}
