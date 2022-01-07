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

import org.apache.commons.configuration2.builder.BasicConfigurationBuilder
import org.apache.commons.configuration2.builder.fluent.Parameters
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler
import org.apache.commons.configuration2.{BaseConfiguration, Configuration}
import org.apache.spark.internal.Logging
import za.co.absa.hyperdrive.driver.IngestionDriver
import za.co.absa.hyperdrive.driver.utils.DriverUtil

object CommandLineIngestionDriver extends IngestionDriver with Logging {

  private val PropertyDelimiter = "="

  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      throw new IllegalArgumentException("No configuration provided.")
    }

    logInfo(s"Starting Hyperdrive ${DriverUtil.getVersionString}")

    logInfo(s"Going to load ${args.length} configurations from command line.")
    val configuration = parseConfiguration(args)
    logInfo("Configuration loaded. Going to invoke ingestion.")
    ingest(configuration)
  }

  def parseConfiguration(settings: Array[String]): Configuration = {
    val configuration = new BasicConfigurationBuilder[BaseConfiguration](classOf[BaseConfiguration])
        .configure(new Parameters()
          .basic()
          .setListDelimiterHandler(new DefaultListDelimiterHandler(ListDelimiter)))
      .getConfiguration

    settings.foreach(setOrThrow(_, configuration))
    configuration
  }

  private def setOrThrow(setting: String, configuration: Configuration): Unit = {
    if(!setting.contains(PropertyDelimiter)) {
      throw new IllegalArgumentException(s"Invalid setting format: $setting")
    } else {
      val settingKeyValue = setting.split(PropertyDelimiter, 2)
      configuration.setProperty(settingKeyValue(0).trim, settingKeyValue(1).trim)
    }
  }
}
