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

package ${package}

import java.util.ServiceLoader

import org.scalatest.{FlatSpec, Matchers}
import za.co.absa.hyperdrive.ingestor.api.decoder.StreamDecoderFactoryProvider
import za.co.absa.hyperdrive.ingestor.api.reader.StreamReaderFactoryProvider
import za.co.absa.hyperdrive.ingestor.api.transformer.StreamTransformerFactoryProvider
import za.co.absa.hyperdrive.ingestor.api.writer.StreamWriterFactoryProvider
import ${package}.decoder.mycomponent.MyStreamDecoderImpl
import ${package}.reader.mycomponent.MyStreamReaderImpl
import ${package}.transformer.mycomponent.MyStreamTransformerImpl
import ${package}.writer.mycomponent.MyStreamWriterImpl

class ServiceProviderConfigurationTest extends FlatSpec with Matchers {
  behavior of "Service Provider Configuration (META-INF/services)"

  it should "load configured factories" in {
    val classLoader = this.getClass.getClassLoader
    import scala.collection.JavaConverters._

    val readerFactory = ServiceLoader.load(classOf[StreamReaderFactoryProvider], classLoader).asScala
      .map(_.getComponentFactory).toList
    readerFactory should contain only MyStreamReaderImpl

    val decoderFactory = ServiceLoader.load(classOf[StreamDecoderFactoryProvider], classLoader).asScala
      .map(_.getComponentFactory).toList
    decoderFactory should contain only MyStreamDecoderImpl

    val transformerFactory = ServiceLoader.load(classOf[StreamTransformerFactoryProvider], classLoader).asScala
      .map(_.getComponentFactory).toList
    transformerFactory should contain only MyStreamTransformerImpl

    val writerFactory = ServiceLoader.load(classOf[StreamWriterFactoryProvider], classLoader).asScala
      .map(_.getComponentFactory).toList
    writerFactory should contain only MyStreamWriterImpl
  }
}
