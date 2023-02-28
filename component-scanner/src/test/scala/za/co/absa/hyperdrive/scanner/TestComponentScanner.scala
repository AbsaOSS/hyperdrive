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

import java.nio.file.{Files, Paths}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.commons.io.TempDirectory
import za.co.absa.hyperdrive.ingestor.api.reader.StreamReaderFactoryProvider
import za.co.absa.hyperdrive.ingestor.api.transformer.StreamTransformerFactoryProvider
import za.co.absa.hyperdrive.ingestor.api.writer.StreamWriterFactoryProvider
import za.co.absa.hyperdrive.scanner.dummyjar._


class TestComponentScanner extends AnyFlatSpec with Matchers with BeforeAndAfter {

  behavior of "ComponentScanner"

  private var baseDirPath = Paths.get(".")

  private val dummyJarPath = "za/co/absa/hyperdrive/scanner/dummyjar/"
  private val dummyPackage = dummyJarPath.replace("/", ".")

  before {
    val baseDir = TempDirectory("TestComponentScanner").deleteOnExit()
    baseDirPath = baseDir.path
  }

  it should "list components in the same jar" in {
    // given
    val filenames = List(
      s"${dummyJarPath}DummyStreamReaderOne.class",
      s"${dummyJarPath}DummyStreamReaderOne$$.class",
      s"${dummyJarPath}DummyStreamReaderOneLoader.class",
      s"${dummyJarPath}DummyStreamReaderTwo.class",
      s"${dummyJarPath}DummyStreamReaderTwo$$.class",
      s"${dummyJarPath}DummyStreamReaderTwoLoader.class",
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
      classOf[StreamTransformerFactoryProvider].getName -> List(s"${dummyPackage}DummyStreamTransformerLoader"),
      classOf[StreamWriterFactoryProvider].getName -> List(s"${dummyPackage}DummyStreamWriterOneLoader"))

    JarTestUtils.createJar(baseDirPath, "jar1.jar", filenames, serviceProviders)

    // when
    val components = ComponentScanner.getComponents(baseDirPath).get

    // then
    val expectedJarPath = baseDirPath.resolve("jar1.jar").toAbsolutePath
    components.readers should contain theSameElementsAs List(
      ComponentDescriptor(DummyStreamReaderOne, s"${dummyPackage}DummyStreamReaderOne$$", expectedJarPath),
      ComponentDescriptor(DummyStreamReaderTwo, s"${dummyPackage}DummyStreamReaderTwo$$", expectedJarPath))
    components.transformers should contain only
      ComponentDescriptor(DummyStreamTransformer, s"${dummyPackage}DummyStreamTransformer$$", expectedJarPath)
    components.writers should contain only
      ComponentDescriptor(DummyStreamWriterOne, s"${dummyPackage}DummyStreamWriterOne$$", expectedJarPath)
  }

  it should "list components in multiple jars" in {
    // given
    val filesJar1 = List(
      s"${dummyJarPath}DummyStreamReaderOne.class",
      s"${dummyJarPath}DummyStreamReaderOne$$.class",
      s"${dummyJarPath}DummyStreamWriterOne.class",
      s"${dummyJarPath}DummyStreamWriterOne$$.class")
    val serviceProviders1 = Map(
      classOf[StreamReaderFactoryProvider].getName -> List(s"${dummyPackage}DummyStreamReaderOneLoader"),
      classOf[StreamWriterFactoryProvider].getName -> List(s"${dummyPackage}DummyStreamWriterOneLoader")
    )
    JarTestUtils.createJar(baseDirPath, "jar1.jar", filesJar1, serviceProviders1)

    val filesJar2 = List(
      s"${dummyJarPath}DummyStreamReaderTwo.class",
      s"${dummyJarPath}DummyStreamReaderTwo$$.class",
      s"${dummyJarPath}DummyStreamWriterTwo.class",
      s"${dummyJarPath}DummyStreamWriterTwo$$.class")
    val serviceProviders2 = Map(
      classOf[StreamReaderFactoryProvider].getName -> List(s"${dummyPackage}DummyStreamReaderTwoLoader"),
      classOf[StreamWriterFactoryProvider].getName -> List(s"${dummyPackage}DummyStreamWriterTwoLoader")
    )
    JarTestUtils.createJar(baseDirPath, "jar2.jar", filesJar2, serviceProviders2)

    // when
    val components = ComponentScanner.getComponents(baseDirPath).get

    // then
    val expectedJar1 = baseDirPath.resolve("jar1.jar").toAbsolutePath
    val expectedJar2 = baseDirPath.resolve("jar2.jar").toAbsolutePath
    components.readers should contain theSameElementsAs List(
      ComponentDescriptor(DummyStreamReaderOne, s"${dummyPackage}DummyStreamReaderOne$$", expectedJar1),
      ComponentDescriptor(DummyStreamReaderTwo, s"${dummyPackage}DummyStreamReaderTwo$$", expectedJar2)
    )

    components.writers should contain theSameElementsAs List(
      ComponentDescriptor(DummyStreamWriterOne, s"${dummyPackage}DummyStreamWriterOne$$", expectedJar1),
      ComponentDescriptor(DummyStreamWriterTwo, s"${dummyPackage}DummyStreamWriterTwo$$", expectedJar2)
    )
  }

  it should "return an empty list if the given directory contains only jar files without class files" in {
    // given
    JarTestUtils.createJar(baseDirPath, "jar1.jar", List())

    // when
    val components = ComponentScanner.getComponents(baseDirPath).get

    // then
    components.readers shouldBe empty
    components.transformers shouldBe empty
    components.writers shouldBe empty
  }

  it should "skip but not fail if a jar is not a zip file" in {
    // given
    Files.createTempFile(Paths.get(baseDirPath.toUri), "notAZipFile", ".jar")

    val filesJar = List(
      s"${dummyJarPath}DummyStreamWriterTwo.class",
      s"${dummyJarPath}DummyStreamWriterTwo$$.class")
    val serviceProviders = Map(
      classOf[StreamWriterFactoryProvider].getName -> List(s"${dummyPackage}DummyStreamWriterTwoLoader")
    )
    JarTestUtils.createJar(baseDirPath, "jar2.jar", filesJar, serviceProviders)

    // when
    val components = ComponentScanner.getComponents(baseDirPath).get

    // then
    val expectedJar = baseDirPath.resolve("jar2.jar").toAbsolutePath
    components.writers should contain only
      ComponentDescriptor(DummyStreamWriterTwo, s"${dummyPackage}DummyStreamWriterTwo$$", expectedJar)
  }

  it should "skip but not fail if the SPI points to a non-existent implementation" in {
    // given
    val filesFakeJar = List(
      s"${dummyJarPath}DummyStreamWriterOne.class",
      s"${dummyJarPath}DummyStreamWriterOne$$.class")
    val fakeServiceProviders = Map(
      classOf[StreamWriterFactoryProvider].getName -> List(s"${dummyPackage}NonExistentLoader")
    )
    JarTestUtils.createJar(baseDirPath, "jar1.jar", filesFakeJar, fakeServiceProviders)

    val filesJar = List(
      s"${dummyJarPath}DummyStreamWriterTwo.class",
      s"${dummyJarPath}DummyStreamWriterTwo$$.class")
    val serviceProviders = Map(
      classOf[StreamWriterFactoryProvider].getName -> List(s"${dummyPackage}DummyStreamWriterTwoLoader")
    )
    JarTestUtils.createJar(baseDirPath, "jar2.jar", filesJar, serviceProviders)

    // when
    val components = ComponentScanner.getComponents(baseDirPath).get

    // then
    val expectedJar = baseDirPath.resolve("jar2.jar").toAbsolutePath
    components.writers should contain only
      ComponentDescriptor(DummyStreamWriterTwo, s"${dummyPackage}DummyStreamWriterTwo$$", expectedJar)
  }


  it should "return a failure if the given path does not exist" in {
    // given
    val baseDirPath = Files.createTempDirectory("directorynotexist")
    Files.delete(baseDirPath)

    // when
    val result = ComponentScanner.getComponents(baseDirPath)

    // then
    result.isFailure shouldBe true
    result.failed.get.getClass shouldBe classOf[IllegalArgumentException]
    result.failed.get.getMessage should fullyMatch regex "Directory .*directorynotexist.* does not exist"
  }

  it should "return a failure if the given path is not a directory" in {
    // given
    val anyFilePath = Files.createTempFile("anyFile", ".tmp")

    // when
    val result = ComponentScanner.getComponents(anyFilePath)

    // then
    result.isFailure shouldBe true
    result.failed.get.getClass shouldBe classOf[IllegalArgumentException]
    result.failed.get.getMessage should fullyMatch regex "Argument .*anyFile.*tmp is not a directory"

    // cleanup
    Files.delete(anyFilePath)
  }
}

