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

package za.co.absa.hyperdrive.ingestor.implementation.reader.factories.kafka

import org.apache.commons.configuration2.Configuration
import org.apache.logging.log4j.LogManager
import za.co.absa.hyperdrive.ingestor.api.reader.StreamReader
import za.co.absa.hyperdrive.ingestor.implementation.reader.StreamReaderFactory
import za.co.absa.hyperdrive.ingestor.implementation.reader.kafka.KafkaStreamReader
import za.co.absa.hyperdrive.shared.utils.ConfigUtils._
import za.co.absa.hyperdrive.shared.configurations.ConfigurationsKeys.KafkaStreamReaderKeys._

private[factories] object KafkaStreamReaderFactory extends StreamReaderFactory {

  private val logger = LogManager.getLogger

  def name = "KafkaStreamReader"

  override def build(conf: Configuration): StreamReader = {
    val topic = getTopic(conf)
    val brokers = getBrokers(conf)
    val extraOptions = getExtraOptions(conf)

    logger.info(s"Going to create KafkaStreamReader with: topic='$topic', brokers='$brokers', extraOptions=${filterKeysContaining(extraOptions, exclusionToken = "password")}")

    new KafkaStreamReader(topic, brokers, extraOptions)
  }

  private def getTopic(configuration: Configuration): String = {
    getOrThrow(KEY_TOPIC, configuration, errorMessage = s"Topic not found. Is $KEY_TOPIC defined?")
  }

  private def getBrokers(configuration: Configuration): String = {
    val brokers = getSeqOrThrow(KEY_BROKERS, configuration, errorMessage = s"Brokers not found. Is $KEY_BROKERS defined?")
    brokers.mkString(",")
  }

  private def getExtraOptions(configuration: Configuration): Map[String,String] = {
    val securityKeys = Seq(KEY_SECURITY_PROTOCOL, KEY_TRUSTSTORE_LOCATION, KEY_TRUSTSTORE_PASSWORD, KEY_KEYSTORE_LOCATION, KEY_KEYSTORE_PASSWORD, KEY_KEY_PASSWORD)

    val extraConfs = securityKeys.foldLeft(Map[String,String]()) {
      case (map,securityKey) =>
        getOrNone(securityKey, configuration) match {
          case Some(value) => map + (tweakKeyName(securityKey) -> value)
          case None => map
        }
    }

    if (extraConfs.isEmpty || extraConfs.size == securityKeys.size) {
      extraConfs
    }
    else {
      logger.warn(s"Assuming no security settings, since some appear to be missing: {${findMissingKeys(securityKeys, extraConfs)}}")
      Map[String,String]()
    }
  }

  private def findMissingKeys(keys: Seq[String], map: Map[String,String]): Seq[String] = keys.filterNot(map.contains(_))

  private def tweakKeyName(key: String): String = {
    key.replace(s"$rootComponentConfKey.", "") // remove the component root configuration key
  }

  private def filterKeysContaining(map: Map[String,String], exclusionToken: String): Map[String,String] = map.filterKeys(!_.contains(exclusionToken))
}
