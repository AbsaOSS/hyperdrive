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

package za.co.absa.hyperdrive.ingestor.implementation.transformer.deduplicate.kafka

import za.co.absa.hyperdrive.ingestor.api.{HasComponentAttributes, PropertyMetadata}

trait DeduplicateKafkaSinkTransformerAttributes extends HasComponentAttributes {

  val SourceIdColumns = "source.id.columns"
  val DestinationIdColumns = "destination.id.columns"
  val KafkaConsumerTimeout = "kafka.consumer.timeout"

  override def getName: String = "Deduplicate Kafka Sink Transformer"

  override def getDescription: String = "This transformer deduplicates records in a Kafka-to-Kafka query in a rerun after a failure." +
    "It is assumed that only one query writes to the sink and no records have been written to the sink since the failure."

  override def getProperties: Map[String, PropertyMetadata] = Map(
    SourceIdColumns -> PropertyMetadata("Source Id columns", Some("Comma separated list of columns that represent the id"), required = true),
    DestinationIdColumns -> PropertyMetadata("Destination Id columns", Some("Comma separated list of columns that represent the id"), required = true),
    KafkaConsumerTimeout -> PropertyMetadata("Kafka consumer timeout in seconds", None, required = false)
  )
}
