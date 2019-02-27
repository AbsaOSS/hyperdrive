/*
 * Copyright 2018 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.hyperdrive.test.settings

import za.co.absa.abris.avro.read.confluent.SchemaManager

object InfrastructureSettings {

  object HyperdriveSettings {
    val NOTIFICATION_TOPIC = "notification_topic"
    val PAYLOAD_TOPIC_1    = "payload_topic_1"
    val PAYLOAD_TOPIC_2    = "payload_topic_2"

    var PAYLOAD_TOPIC_IN_USE = PAYLOAD_TOPIC_1

    // THIS IS HERE FOR QUICK TESTS ONLY, WILL GO AWAY ASAP
    def getPayloadDestinationDir(): String = {
      if (PAYLOAD_TOPIC_IN_USE == PAYLOAD_TOPIC_1)
        "/tmp/dest1"
      else if (PAYLOAD_TOPIC_IN_USE == PAYLOAD_TOPIC_2)
        "/tmp/dest2"
      else "/tmp/INCORRECT_DESTINATION"
    }
  }

  object SparkSettings {
    val CHECKPOINT_LOCATION_KEY  = "checkpointLocation"
    val CHECKPOINT_BASE_LOCATION = "/tmp/ingestion_checkpoint"
    val FAIL_ON_DATA_LOSS_KEY    = "failOnDataLoss"
  }

  object KafkaSettings {
    val STREAM_FORMAT_KAFKA_NAME  = "kafka"
    val BROKERS                   = "PLAINTEXT://localhost:9092"
    val BROKERS_SETTING_KEY       = "bootstrap.servers"
    val SPARK_BROKERS_SETTING_KEY = "kafka.bootstrap.servers"

    val TOPIC_SUBSCRIPTION_KEY    = "subscribe"
    val TOPIC_DISPATCH_KEY        = "topic"
    val STARTING_OFFSETS_KEY      = "startingOffsets"

    val KEY_DESERIALIZER_KEY      = "key.deserializer"
    val KEY_DESERIALIZER          = "org.apache.kafka.common.serialization.StringDeserializer"

    val VALUE_DESERIALIZER_KEY    = "value.deserializer"
    val VALUE_DESERIALIZER        = "org.apache.kafka.common.serialization.ByteArrayDeserializer"

    val GROUP_ID_KEY              = "group.id"
  }

  object SchemaRegistrySettings {
    val URL                          = "http://localhost:8081"
    val KEY_SCHEMA_NAMING_STRATEGY   = SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME
    val VALUE_SCHEMA_NAMING_STRATEGY = SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME

    val SCHEMA_REGISTRY_ACCESS_SETTINGS = Map(
      SchemaManager.PARAM_SCHEMA_REGISTRY_URL          -> URL,
      SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> VALUE_SCHEMA_NAMING_STRATEGY,
      SchemaManager.PARAM_KEY_SCHEMA_NAMING_STRATEGY   -> VALUE_SCHEMA_NAMING_STRATEGY
    )
  }

  object AvroSettings {
    val GENERAL_SCHEMA_NAME      = "schemaName"
    val GENERAL_SCHEMA_NAMESPACE = "schemaNamespace"
  }
}
