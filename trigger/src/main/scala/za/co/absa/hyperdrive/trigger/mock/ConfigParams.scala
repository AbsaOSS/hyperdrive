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

package za.co.absa.hyperdrive.trigger.mock

import za.co.absa.abris.avro.read.confluent.SchemaManager
import za.co.absa.abris.avro.read.confluent.SchemaManager.SchemaStorageNamingStrategies

object ConfigParams {

  val KAFKA_BROKERS = "PLAINTEXT://localhost:9092"
  val CHECKPOINT_BASE_LOCATION = "/tmp/HYPERDRIVE_CHECKPOINT"
  var SCHEMA_REGISTRY_URL = "http://localhost:8081"

  lazy val SCHEMA_REGISTRY_ACCESS_SETTINGS: Map[String,String] = Map(
    SchemaManager.PARAM_SCHEMA_REGISTRY_URL          -> SCHEMA_REGISTRY_URL,
    SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> SchemaStorageNamingStrategies.TOPIC_NAME,
    SchemaManager.PARAM_VALUE_SCHEMA_ID -> "latest"
  )
}
