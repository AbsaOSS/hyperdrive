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

package za.co.absa.hyperdrive.ingestor.implementation.utils

import org.apache.commons.configuration2.Configuration
import za.co.absa.abris.config.AbrisConfig
import za.co.absa.hyperdrive.ingestor.api.utils.ConfigUtils
import za.co.absa.hyperdrive.ingestor.api.utils.ConfigUtils.{getOrNone, getOrThrow}
import za.co.absa.hyperdrive.ingestor.implementation.transformer.avro.confluent.SchemaRegistryAttributes

import scala.io.Source

private[hyperdrive] object SchemaRegistryConfigUtil {
  val BasicAuthUserInfoKey = "basic.auth.user.info"
  def getSchemaRegistryConfig(config: Configuration): Map[String, String] = {
    Map(AbrisConfig.SCHEMA_REGISTRY_URL -> getSchemaRegistryUrl(config)) ++
      getBasicAuthUserInfo(config).map(userInfo => Map(BasicAuthUserInfoKey -> userInfo)).getOrElse(Map()) ++
      getExtraConfig(config)
  }

  private def getSchemaRegistryUrl(config: Configuration) = {
    val key = SchemaRegistryAttributes.KEY_SCHEMA_REGISTRY_URL
    getOrThrow(key, config, errorMessage = s"Schema Registry URL not specified. Is '${key}' configured?")
  }

  private def getBasicAuthUserInfo(config: Configuration) = {
    getOrNone(SchemaRegistryAttributes.KEY_SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO_FILE, config).map(file => {
      val source = Source.fromFile(file)
      try {
       source.getLines().toList.headOption.map(_.trim).getOrElse(throw new IllegalArgumentException(
          s"The file ${SchemaRegistryAttributes.KEY_SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO_FILE} was empty, " +
            s"expected one line in the form <username>:<password>"))
      } finally {
        source.close()
      }
    })
  }

  private def getExtraConfig(config: Configuration) =
    ConfigUtils.getPropertySubset(config, SchemaRegistryAttributes.KEY_SCHEMA_REGISTRY_EXTRA_CONFS_ROOT)

}
