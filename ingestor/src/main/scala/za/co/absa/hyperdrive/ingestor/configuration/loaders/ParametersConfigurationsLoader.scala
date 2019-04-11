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

package za.co.absa.hyperdrive.ingestor.configuration.loaders

import java.util.Properties

import org.apache.logging.log4j.LogManager
import za.co.absa.hyperdrive.ingestor.configuration.CompositeIngestionConfigurations
import za.co.absa.hyperdrive.ingestor.configuration.loaders.ConfigurationsKeys.{StreamDecoderKeys, StreamReaderKeys, StreamWriterKeys}

/**
  * This loader loads the configurations from an array of parameters
  */
private[configuration] object ParametersConfigurationsLoader {

  private val logger = LogManager.getLogger

  private val confMapsKeys = Set(StreamReaderKeys.KEY_EXTRA_CONFS, StreamDecoderKeys.KEY_SCHEMA_REGISTRY_SETTINGS, StreamWriterKeys.KEY_EXTRA_CONFS)

  def load(args: Array[String]): CompositeIngestionConfigurations = {
    if (args.isEmpty) {
      throw new IllegalArgumentException("Empty arguments.")
    }

    logger.info(s"Going to load configurations from arguments: [${args.mkString(" ")}]")

    PropertiesConfigurationsLoader.load(argumentsToProperties(args))
  }

  private def argumentsToProperties(args: Array[String]): Properties = {
    args.foldLeft(new Properties()) {
      case (properties,arg) => {
        val firstEquals = arg.indexOf("=") // done this way since some properties also contain key=value, thus, split would not work as intended
        if (firstEquals == -1) {
          throw new IllegalArgumentException(s"Invalid argument: '$arg'.")
        }
        val key = arg.substring(0, firstEquals)
        val value = arg.substring(firstEquals + 1)
        val formattedValue = formatValueIfListOfConfMaps(key, value) // if value is a list of conf maps (e.g. key1=value1,key2=value2,etc), replaces "," by " "

        setProperty(key.trim, formattedValue.trim, properties)
      }
    }
  }

  private def setProperty(key: String, value: String, properties: Properties): Properties = {
    logger.debug(s"Setting property: key='$key', value='$value'.")
    properties.put(key, value)
    properties
  }

  private def formatValueIfListOfConfMaps(key: String, value: String): String = {
    if (isConfMap(key)) {
      value.replaceAll(",", " ")
    } else {
      value
    }
  }

  private def isConfMap(key: String): Boolean = confMapsKeys.contains(key)
}
