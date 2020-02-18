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

package za.co.absa.hyperdrive.scanner

import java.io.File
import java.nio.file.Files

import org.scalatest.{FlatSpec, Matchers}
import za.co.absa.hyperdrive.ingestor.api.decoder.StreamDecoderFactoryProvider
import za.co.absa.hyperdrive.ingestor.api.manager.StreamManagerFactoryProvider
import za.co.absa.hyperdrive.ingestor.api.reader.StreamReaderFactoryProvider
import za.co.absa.hyperdrive.ingestor.api.transformer.StreamTransformerFactoryProvider
import za.co.absa.hyperdrive.ingestor.api.writer.StreamWriterFactoryProvider
import za.co.absa.hyperdrive.scanner.dummyjar._

import scala.reflect.io.Directory


class TestComponentScanner extends FlatSpec with Matchers {

  behavior of "ComponentScanner"

  private val dummyJarPath = "za/co/absa/hyperdrive/scanner/dummyjar/"
  private val dummyPackage = dummyJarPath.replace("/", ".")

  it should "list API components in the same jar" in {
    // given
    val baseDirPath = Files.createTempDirectory("listAllComponentsInSingleJar")
    val baseDir = new File(baseDirPath.toUri)
    val filenames = List(
      s"${dummyJarPath}DummyStreamReaderOne.class",
      s"${dummyJarPath}DummyStreamReaderOne$$.class",
      s"${dummyJarPath}DummyStreamReaderOneLoader.class",
      s"${dummyJarPath}DummyStreamReaderTwo.class",
      s"${dummyJarPath}DummyStreamReaderTwo$$.class",
      s"${dummyJarPath}DummyStreamReaderTwoLoader.class",
      s"${dummyJarPath}DummyStreamManager.class",
      s"${dummyJarPath}DummyStreamManager$$.class",
      s"${dummyJarPath}DummyStreamManagerLoader.class",
      s"${dummyJarPath}DummyStreamDecoder.class",
      s"${dummyJarPath}DummyStreamDecoder$$.class",
      s"${dummyJarPath}DummyStreamDecoderLoader.class",
      s"${dummyJarPath}DummyStreamTransformer.class",
      s"${dummyJarPath}DummyStreamTransformer$$.class",
      s"${dummyJarPath}DummyStreamTransformerLoader.class",
      s"${dummyJarPath}DummyStreamWriterOne.class",
      s"${dummyJarPath}DummyStreamWriterOne$$.class",
      s"${dummyJarPath}DummyStreamWriterOneLoader.class"
    )

    val serviceProviders = Map(
      classOf[StreamReaderFactoryProvider].getName -> List(
        s"${dummyPackage}DummyStreamReaderOneLoader",
        s"${dummyPackage}DummyStreamReaderTwoLoader"),
      classOf[StreamManagerFactoryProvider].getName -> List(s"${dummyPackage}DummyStreamManagerLoader"),
      classOf[StreamDecoderFactoryProvider].getName -> List(s"${dummyPackage}DummyStreamDecoderLoader"),
      classOf[StreamTransformerFactoryProvider].getName -> List(s"${dummyPackage}DummyStreamTransformerLoader"),
      classOf[StreamWriterFactoryProvider].getName -> List(s"${dummyPackage}DummyStreamWriterOneLoader"))

    JarTestUtils.createJar(baseDir, "jar1.jar", filenames, serviceProviders)

    // when
    val components = ComponentScanner.getComponents(baseDir).get

    // then
    val expectedJarPath = new File(baseDir.getAbsolutePath + "/jar1.jar")
    components.readers should contain theSameElementsAs List(
      ComponentDescriptor(DummyStreamReaderOne, s"${dummyPackage}DummyStreamReaderOne$$", expectedJarPath),
      ComponentDescriptor(DummyStreamReaderTwo, s"${dummyPackage}DummyStreamReaderTwo$$", expectedJarPath))
    components.managers should contain only
      ComponentDescriptor(DummyStreamManager, s"${dummyPackage}DummyStreamManager$$", expectedJarPath)
    components.decoders should contain only
      ComponentDescriptor(DummyStreamDecoder, s"${dummyPackage}DummyStreamDecoder$$", expectedJarPath)
    components.transformers should contain only
      ComponentDescriptor(DummyStreamTransformer, s"${dummyPackage}DummyStreamTransformer$$", expectedJarPath)
    components.writers should contain only
      ComponentDescriptor(DummyStreamWriterOne, s"${dummyPackage}DummyStreamWriterOne$$", expectedJarPath)

    // cleanup
    new Directory(baseDir).deleteRecursively()
  }
}

