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
import org.slf4j.LoggerFactory
import za.co.absa.hyperdrive.driver.IngestionDriver

object CommandLineIngestionDriver extends IngestionDriver {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val PropertyDelimiter = "="

  override def loadConfiguration(args: Array[String]): Configuration = {
    if (args.isEmpty) {
      throw new IllegalArgumentException("No configuration provided.")
    }
    logger.info(s"Going to load ${args.length} configurations from command line.")
    parseConfiguration(args)
  }

  private def parseConfiguration(settings: Array[String]): Configuration = {
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
