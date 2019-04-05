/*
 *  Copyright 2019 ABSA Group Limited
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package za.co.absa.hyperdrive.ingestor.drivers

import org.apache.logging.log4j.LogManager
import za.co.absa.hyperdrive.ingestor.IngestionDriver
import za.co.absa.hyperdrive.ingestor.configuration.ConfigurationsLoadersFacade

/**
  * This driver launches an ingestion by loading the configurations from system properties.
  */
object SystemPropertiesIngestionDriver extends IngestionDriver {

  private val logger = LogManager.getLogger

  def main(args: Array[String]): Unit = {
    logger.info("Going to load configurations from system properties.")
    val configurations = ConfigurationsLoadersFacade.loadFromSystemProperties
    logger.info("Configurations loaded. Going to invoke ingestion.")
    ingest(configurations)
  }
}
