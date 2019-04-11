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

object ConfigurationsKeys {

  object SparkConfKeys {
    val KEY_SPARK_APP_NAME = "spark.app.name"
  }

  object StreamReaderKeys {
    val KEY_EXTRA_CONFS: String = "source.extra.confs"
    val KEY_SOURCE_TOPIC: String = "source.topic"
    val KEY_SOURCE_BROKERS: String = "source.brokers"
  }

  object OffsetManagerKeys {
    val KEY_OFFSET_MANAGER_TOPIC: String = StreamReaderKeys.KEY_SOURCE_TOPIC
    val KEY_CHECKPOINT_BASE_LOCATION: String = "checkpoint.base.location"
  }

  object StreamDecoderKeys {
    val KEY_TOPIC:String = StreamReaderKeys.KEY_SOURCE_TOPIC
    val KEY_SCHEMA_RETENTION_POLICY: String = "schema.retention.policy"
    val KEY_SCHEMA_REGISTRY_SETTINGS: String = "schema.registry.settings"
  }

  object StreamWriterKeys {
    val KEY_EXTRA_CONFS: String = "sink.extra.confs"
    val KEY_DESTINATION_DIRECTORY: String = "destination.directory"
  }

  object StreamTransformerKeys {
    val KEY_TRANSFORMATION_QUERY: String = "stream.tranformation.query"
  }
}
