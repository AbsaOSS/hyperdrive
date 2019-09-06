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

package za.co.absa.hyperdrive.ingestor.implementation.decoder.factories

import org.apache.commons.configuration2.Configuration
import org.mockito.Mockito._
import org.scalatest.FlatSpec
import org.scalatest.mockito.MockitoSugar
import za.co.absa.abris.avro.read.confluent.SchemaManager
import za.co.absa.abris.avro.schemas.policy.SchemaRetentionPolicies
import za.co.absa.hyperdrive.ingestor.implementation.decoder.avro.confluent.ConfluentAvroKafkaStreamDecoder
import za.co.absa.hyperdrive.shared.configurations.ConfigurationsKeys.AvroKafkaStreamDecoderKeys._


class TestStreamDecoderAbstractFactory extends FlatSpec with MockitoSugar {

  private val configStub = mock[Configuration]

  behavior of StreamDecoderAbstractFactory.getClass.getSimpleName

  it should "create AvroStreamDecoder" in {
    when(configStub.getString(StreamDecoderAbstractFactory.componentConfigKey)).thenReturn(ConfluentAvroKafkaStreamDecoder.getClass.getName)
    when(configStub.getString(KEY_TOPIC)).thenReturn("topic")
    when(configStub.getString(KEY_SCHEMA_REGISTRY_URL)).thenReturn("http://localhost:8081")
    when(configStub.getString(KEY_SCHEMA_REGISTRY_VALUE_SCHEMA_ID)).thenReturn("latest")
    when(configStub.getString(KEY_SCHEMA_REGISTRY_VALUE_NAMING_STRATEGY)).thenReturn(SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME)
    when(configStub.getString(KEY_SCHEMA_RETENTION_POLICY)).thenReturn(SchemaRetentionPolicies.RETAIN_ORIGINAL_SCHEMA.toString)

    assert(StreamDecoderAbstractFactory.build(configStub).isInstanceOf[ConfluentAvroKafkaStreamDecoder])
  }

  it should "throw if invalid decoder type is specified" in {
    val invalidDecoderType = "invalid.decoder-type"
    when(configStub.getString(StreamDecoderAbstractFactory.componentConfigKey)).thenReturn(invalidDecoderType)
    val throwable = intercept[IllegalArgumentException](StreamDecoderAbstractFactory.build(configStub))
    assert(throwable.getMessage.toLowerCase.contains(invalidDecoderType))
  }

  it should "throw if no decoder type specified" in {
    assertThrows[IllegalArgumentException](StreamDecoderAbstractFactory.build(configStub))
  }
}
