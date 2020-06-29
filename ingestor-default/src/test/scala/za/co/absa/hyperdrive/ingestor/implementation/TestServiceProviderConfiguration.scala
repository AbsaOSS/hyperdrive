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

package za.co.absa.hyperdrive.ingestor.implementation

import java.util.ServiceLoader

import org.scalatest.{FlatSpec, Matchers}
import za.co.absa.hyperdrive.ingestor.api.decoder.{StreamDecoderFactory, StreamDecoderFactoryProvider}
import za.co.absa.hyperdrive.ingestor.api.reader.{StreamReaderFactory, StreamReaderFactoryProvider}
import za.co.absa.hyperdrive.ingestor.api.transformer.{StreamTransformerFactory, StreamTransformerFactoryProvider}
import za.co.absa.hyperdrive.ingestor.api.writer.{StreamWriterFactory, StreamWriterFactoryProvider}
import za.co.absa.hyperdrive.ingestor.api.{ComponentFactory, ComponentFactoryProvider}
import za.co.absa.hyperdrive.ingestor.implementation.decoder.avro.confluent.ConfluentAvroKafkaStreamDecoder
import za.co.absa.hyperdrive.ingestor.implementation.reader.kafka.KafkaStreamReader
import za.co.absa.hyperdrive.ingestor.implementation.transformer.column.selection.ColumnSelectorStreamTransformer
import za.co.absa.hyperdrive.ingestor.implementation.transformer.dateversion.AddDateVersionTransformer
import za.co.absa.hyperdrive.ingestor.implementation.writer.kafka.KafkaStreamWriter
import za.co.absa.hyperdrive.ingestor.implementation.writer.parquet.ParquetStreamWriter

import scala.reflect.ClassTag

class TestServiceProviderConfiguration extends FlatSpec with Matchers {

  behavior of "Service Provider Interface (META-INF/services)"

  it should "load ConfluentAvroKafkaStreamDecoder" in {
    val factoryProviders = loadServices[StreamDecoderFactoryProvider, StreamDecoderFactory]()
    factoryProviders should contain only ConfluentAvroKafkaStreamDecoder
  }

  it should "load KafkaStreamReader" in {
    val factoryProviders = loadServices[StreamReaderFactoryProvider, StreamReaderFactory]()
    factoryProviders should contain only KafkaStreamReader
  }

  it should "load StreamTransformers" in {
    val factoryProviders = loadServices[StreamTransformerFactoryProvider, StreamTransformerFactory]()
    factoryProviders should contain theSameElementsAs Seq(AddDateVersionTransformer, ColumnSelectorStreamTransformer)
  }

  it should "load StreamWriters" in {
    val factoryProviders = loadServices[StreamWriterFactoryProvider, StreamWriterFactory]()
    factoryProviders should contain theSameElementsAs Seq(ParquetStreamWriter, KafkaStreamWriter)
  }

  private def loadServices[P <: ComponentFactoryProvider[F], F <: ComponentFactory[_]]()(implicit classTag: ClassTag[P]): Iterable[F] = {
    val classLoader = this.getClass.getClassLoader
    import scala.collection.JavaConverters._
    ServiceLoader.load(classTag.runtimeClass, classLoader)
      .asScala
      .map(_.asInstanceOf[P])
      .map(_.getComponentFactory)
      .toList
  }
}
