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

package za.co.absa.hyperdrive.shared.configurations

object ConfigurationsKeys {

  object HyperdriveComponentsKeys {
    val rootComponentConfKey = "component"

    val KEY_INGESTOR = s"$rootComponentConfKey.ingestor"
    val KEY_READER = s"$rootComponentConfKey.reader"
    val KEY_DECODER = s"$rootComponentConfKey.decoder"
    val KEY_MANAGER = s"$rootComponentConfKey.manager"
    val KEY_TRANSFORMER = s"$rootComponentConfKey.transformer"
    val KEY_WRITER = s"$rootComponentConfKey.writer"
  }

  object IngestorKeys {
    val rootComponentConfKey = "ingestor"
    val rootFactoryConfKey = s"$rootComponentConfKey.spark"

    val KEY_APP_NAME = s"$rootFactoryConfKey.app.name"
  }

  object KafkaStreamReaderKeys {
    val rootComponentConfKey = "reader"
    val rootFactoryConfKey = s"$rootComponentConfKey.kafka"

    val KEY_TOPIC = s"$rootFactoryConfKey.topic"
    val KEY_BROKERS = s"$rootFactoryConfKey.brokers"
    val KEY_SECURITY_PROTOCOL = s"$rootFactoryConfKey.security.protocol"
    val KEY_TRUSTSTORE_LOCATION = s"$rootFactoryConfKey.ssl.truststore.location"
    val KEY_TRUSTSTORE_PASSWORD = s"$rootFactoryConfKey.ssl.truststore.password"
    val KEY_KEYSTORE_LOCATION = s"$rootFactoryConfKey.ssl.keystore.location"
    val KEY_KEYSTORE_PASSWORD = s"$rootFactoryConfKey.ssl.keystore.password"
    val KEY_KEY_PASSWORD = s"$rootFactoryConfKey.ssl.key.password"
  }

  object ColumnSelectorStreamTransformerKeys {
    val KEY_COLUMNS_TO_SELECT = "transformer.columns.to.select"
  }

  object AvroKafkaStreamDecoderKeys {
    val rootComponentRootKey = "decoder"
    val rootFactoryConfKey = s"$rootComponentRootKey.avro"

    val KEY_TOPIC: String = KafkaStreamReaderKeys.KEY_TOPIC
    val KEY_SCHEMA_REGISTRY_URL = s"$rootFactoryConfKey.schema.registry.url"
    val KEY_SCHEMA_REGISTRY_VALUE_NAMING_STRATEGY = s"$rootFactoryConfKey.value.schema.naming.strategy"
    val KEY_SCHEMA_REGISTRY_VALUE_SCHEMA_ID = s"$rootFactoryConfKey.value.schema.id"
    val KEY_SCHEMA_RETENTION_POLICY = s"$rootFactoryConfKey.schema.retention.policy"
  }

  object CheckpointOffsetManagerKeys {
    val rootComponentRootKey = "manager"
    val rootFactoryConfKey = s"$rootComponentRootKey.checkpoint"

    val KEY_TOPIC: String = KafkaStreamReaderKeys.KEY_TOPIC
    val KEY_CHECKPOINT_BASE_LOCATION = s"$rootFactoryConfKey.base.location"
  }

  object ParquetStreamWriterKeys {
    val rootComponentRootKey = "writer"
    val rootFactoryConfKey = s"$rootComponentRootKey.parquet"

    val KEY_DESTINATION_DIRECTORY = s"$rootFactoryConfKey.destination.directory"
    val KEY_EXTRA_CONFS_ROOT = s"$rootFactoryConfKey.extra.conf"
  }
}
