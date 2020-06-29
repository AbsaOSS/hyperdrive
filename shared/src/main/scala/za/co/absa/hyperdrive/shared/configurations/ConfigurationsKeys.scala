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

package za.co.absa.hyperdrive.shared.configurations

private[hyperdrive] object ConfigurationsKeys {

  object HyperdriveComponentsKeys {
    val rootComponentConfKey = "component"

    val KEY_INGESTOR = s"$rootComponentConfKey.ingestor"
    val KEY_READER = s"$rootComponentConfKey.reader"
    val KEY_DECODER = s"$rootComponentConfKey.decoder"
    val KEY_TRANSFORMER = s"$rootComponentConfKey.transformer"
    val KEY_WRITER = s"$rootComponentConfKey.writer"
  }

  object KafkaStreamReaderKeys {
    val rootComponentConfKey = "reader"
    val rootFactoryConfKey = s"$rootComponentConfKey.kafka"
    val rootFactoryOptionalConfKey = s"$rootComponentConfKey.option"
    val rootFactoryOptionalKafkaKey = s"$rootFactoryOptionalConfKey.kafka"

    val KEY_TOPIC = s"$rootFactoryConfKey.topic"
    val KEY_BROKERS = s"$rootFactoryConfKey.brokers"
    val KEY_SECURITY_PROTOCOL = s"$rootFactoryOptionalKafkaKey.security.protocol"
    val KEY_TRUSTSTORE_LOCATION = s"$rootFactoryOptionalKafkaKey.ssl.truststore.location"
    val KEY_TRUSTSTORE_PASSWORD = s"$rootFactoryOptionalKafkaKey.ssl.truststore.password"
    val KEY_KEYSTORE_LOCATION = s"$rootFactoryOptionalKafkaKey.ssl.keystore.location"
    val KEY_KEYSTORE_PASSWORD = s"$rootFactoryOptionalKafkaKey.ssl.keystore.password"
    val KEY_KEY_PASSWORD = s"$rootFactoryOptionalKafkaKey.ssl.key.password"
  }
  object ColumnSelectorStreamTransformerKeys {
    val KEY_COLUMNS_TO_SELECT = "columns.to.select"
  }

  object AvroKafkaStreamDecoderKeys {
    val rootComponentRootKey = "decoder"
    val rootFactoryConfKey = s"$rootComponentRootKey.avro"

    val KEY_TOPIC: String = KafkaStreamReaderKeys.KEY_TOPIC
    val KEY_SCHEMA_REGISTRY_URL = s"$rootFactoryConfKey.schema.registry.url"

    val KEY_SCHEMA_REGISTRY_VALUE_NAMING_STRATEGY = s"$rootFactoryConfKey.value.schema.naming.strategy"
    val KEY_SCHEMA_REGISTRY_VALUE_SCHEMA_ID = s"$rootFactoryConfKey.value.schema.id"
    val KEY_SCHEMA_REGISTRY_VALUE_RECORD_NAME = s"$rootFactoryConfKey.value.schema.record.name"
    val KEY_SCHEMA_REGISTRY_VALUE_RECORD_NAMESPACE = s"$rootFactoryConfKey.value.schema.record.namespace"

    val KEY_CONSUME_KEYS = s"$rootFactoryConfKey.consume.keys"
    val KEY_SCHEMA_REGISTRY_KEY_NAMING_STRATEGY = s"$rootFactoryConfKey.key.schema.naming.strategy"
    val KEY_SCHEMA_REGISTRY_KEY_SCHEMA_ID = s"$rootFactoryConfKey.key.schema.id"
    val KEY_SCHEMA_REGISTRY_KEY_RECORD_NAME = s"$rootFactoryConfKey.key.schema.record.name"
    val KEY_SCHEMA_REGISTRY_KEY_RECORD_NAMESPACE = s"$rootFactoryConfKey.key.schema.record.namespace"
  }

  object ParquetStreamWriterKeys {
    val rootComponentRootKey = "writer"
    val rootFactoryConfKey = s"$rootComponentRootKey.parquet"

    val KEY_DESTINATION_DIRECTORY = s"$rootFactoryConfKey.destination.directory"
    val KEY_PARTITION_COLUMNS = s"$rootFactoryConfKey.partition.columns"
    val KEY_METADATA_CHECK = s"$rootFactoryConfKey.metadata.check"
    val KEY_EXTRA_CONFS_ROOT = s"$rootFactoryConfKey.options"
  }
}
