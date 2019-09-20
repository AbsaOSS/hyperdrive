/*
 *  Copyright 2019 ABSA Group Limited
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *s
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package za.co.absa.hyperdrive.scanner

import java.io.File
import java.nio.file.{Files, Path}

import org.scalatest.{FlatSpec, Matchers}
import za.co.absa.hyperdrive.ingestor.api.decoder.StreamDecoderFactory
import za.co.absa.hyperdrive.ingestor.api.manager.OffsetManagerFactory
import za.co.absa.hyperdrive.ingestor.api.reader.StreamReaderFactory
import za.co.absa.hyperdrive.ingestor.api.transformer.StreamTransformerFactory
import za.co.absa.hyperdrive.ingestor.api.writer.StreamWriterFactory

import scala.reflect.io.Directory
import scala.reflect.runtime.{universe => ru}


class TestObjectScanner extends FlatSpec with Matchers {

  behavior of "ObjectScanner"

  val DUMMYJARPATH = "za/co/absa/hyperdrive/scanner/dummyjar/"
  val DUMMYPACKAGE = "za.co.absa.hyperdrive.scanner.dummyjar."

  it should "list objects in the same jar" in {
    // given
    val baseDirPath = Files.createTempDirectory("listAllComponentsInSingleJar")
    val baseDir = new File(baseDirPath.toUri)
    val filenames = List(
      s"${DUMMYJARPATH}DummyStreamReaderOne.class",
      s"${DUMMYJARPATH}DummyStreamReaderOne$$.class",
      s"${DUMMYJARPATH}DummyOffsetManager.class",
      s"${DUMMYJARPATH}DummyOffsetManager$$.class",
      s"${DUMMYJARPATH}DummyStreamDecoder.class",
      s"${DUMMYJARPATH}DummyStreamDecoder$$.class",
      s"${DUMMYJARPATH}DummyStreamTransformer.class",
      s"${DUMMYJARPATH}DummyStreamTransformer$$.class",
      s"${DUMMYJARPATH}DummyStreamWriterOne.class",
      s"${DUMMYJARPATH}DummyStreamWriterOne$$.class")
    JarTestUtils.createJar(baseDir, "jar1.jar", filenames)

    // when
    val readers = ObjectScanner.getObjectsInfo(baseDir, ru.symbolOf[StreamReaderFactory]).get
    val managers = ObjectScanner.getObjectsInfo(baseDir, ru.symbolOf[OffsetManagerFactory]).get
    val decoders = ObjectScanner.getObjectsInfo(baseDir, ru.symbolOf[StreamDecoderFactory]).get
    val transformers = ObjectScanner.getObjectsInfo(baseDir, ru.symbolOf[StreamTransformerFactory]).get
    val writers = ObjectScanner.getObjectsInfo(baseDir, ru.symbolOf[StreamWriterFactory]).get

    // then
    val expectedJarPath = baseDir.getAbsolutePath + "/jar1.jar"
    readers should contain only ((s"${DUMMYPACKAGE}DummyStreamReaderOne$$", expectedJarPath))
    managers should contain only ((s"${DUMMYPACKAGE}DummyOffsetManager$$", expectedJarPath))
    decoders should contain only ((s"${DUMMYPACKAGE}DummyStreamDecoder$$", expectedJarPath))
    transformers should contain only ((s"${DUMMYPACKAGE}DummyStreamTransformer$$", expectedJarPath))
    writers should contain only ((s"${DUMMYPACKAGE}DummyStreamWriterOne$$", expectedJarPath))

    // cleanup
    new Directory(baseDir).deleteRecursively()
  }

  it should "list objects in multiple jars" in {
    // given
    val baseDirPath = Files.createTempDirectory("listMultipleComponentsInMultipleJars")
    val baseDir = new File(baseDirPath.toUri)
    val filesJar1 = List(
      s"${DUMMYJARPATH}DummyStreamReaderOne.class",
      s"${DUMMYJARPATH}DummyStreamReaderOne$$.class",
      s"${DUMMYJARPATH}DummyStreamWriterOne.class",
      s"${DUMMYJARPATH}DummyStreamWriterOne$$.class")
    JarTestUtils.createJar(baseDir, "jar1.jar", filesJar1)

    val filesJar2 = List(
      s"${DUMMYJARPATH}DummyStreamReaderTwo.class",
      s"${DUMMYJARPATH}DummyStreamReaderTwo$$.class",
      s"${DUMMYJARPATH}DummyStreamWriterTwo.class",
      s"${DUMMYJARPATH}DummyStreamWriterTwo$$.class")
    JarTestUtils.createJar(baseDir, "jar2.jar", filesJar2)

    // when
    val readers = ObjectScanner.getObjectsInfo(baseDir, ru.symbolOf[StreamReaderFactory]).get
    val writers = ObjectScanner.getObjectsInfo(baseDir, ru.symbolOf[StreamWriterFactory]).get

    // then
    readers should contain theSameElementsAs List(
      (s"${DUMMYPACKAGE}DummyStreamReaderOne$$", baseDir.getAbsolutePath + "/jar1.jar"),
      (s"${DUMMYPACKAGE}DummyStreamReaderTwo$$", baseDir.getAbsolutePath + "/jar2.jar"))

    writers should contain theSameElementsAs List(
      (s"${DUMMYPACKAGE}DummyStreamWriterOne$$", baseDir.getAbsolutePath + "/jar1.jar"),
      (s"${DUMMYPACKAGE}DummyStreamWriterTwo$$", baseDir.getAbsolutePath + "/jar2.jar"))

    // cleanup
    new Directory(baseDir).deleteRecursively()
  }

  it should "list objects of the same type in the same jar" in {
    // given
    val baseDirPath = Files.createTempDirectory("listMultipleComponentsInSingleJar")
    val baseDir = new File(baseDirPath.toUri)
    val files = List(
      s"${DUMMYJARPATH}DummyStreamReaderOne.class",
      s"${DUMMYJARPATH}DummyStreamReaderOne$$.class",
      s"${DUMMYJARPATH}DummyStreamReaderTwo.class",
      s"${DUMMYJARPATH}DummyStreamReaderTwo$$.class")
    JarTestUtils.createJar(baseDir, "jar1.jar", files)

    // when
    val readers = ObjectScanner.getObjectsInfo(baseDir, ru.symbolOf[StreamReaderFactory]).get

    // then
    readers should contain theSameElementsAs List(
      (s"${DUMMYPACKAGE}DummyStreamReaderOne$$", baseDir.getAbsolutePath + "/jar1.jar"),
      (s"${DUMMYPACKAGE}DummyStreamReaderTwo$$", baseDir.getAbsolutePath + "/jar1.jar"))

    // cleanup
    new Directory(baseDir).deleteRecursively()
  }

  it should "not list any abstract classes or traits" in {
    // given
    val baseDirPath = Files.createTempDirectory("listNoAbstractClassesOrTraits")
    val baseDir = new File(baseDirPath.toUri)
    val files = List(
      s"${DUMMYJARPATH}DummyStreamReaderOne.class",
      s"${DUMMYJARPATH}DummyStreamReaderOne$$.class",
      s"${DUMMYJARPATH}DummyStreamReaderTwo.class",
      s"${DUMMYJARPATH}DummyStreamReaderTwo$$.class",
      s"${DUMMYJARPATH}AbstractDummyStreamReaderFactory.class",
      s"${DUMMYJARPATH}DummyTrait.class")
    JarTestUtils.createJar(baseDir, "jar1.jar", files)

    // when
    val readers = ObjectScanner.getObjectsInfo(baseDir, ru.symbolOf[StreamReaderFactory]).get

    // then
    readers should contain theSameElementsAs List(
      (s"${DUMMYPACKAGE}DummyStreamReaderOne$$", baseDir.getAbsolutePath + "/jar1.jar"),
      (s"${DUMMYPACKAGE}DummyStreamReaderTwo$$", baseDir.getAbsolutePath + "/jar1.jar"))

    // cleanup
    new Directory(baseDir).deleteRecursively()
  }

  it should "return an empty list if the given directory contains only jar files without class files" in {
    // given
    val baseDirPath = Files.createTempDirectory("jarswithoutclassfiles")
    val baseDir = new File(baseDirPath.toUri)
    JarTestUtils.createJar(baseDir, "jar1.jar", List())

    // when
    val readers = ObjectScanner.getObjectsInfo(baseDir, ru.symbolOf[StreamReaderFactory]).get

    // then
    readers shouldBe empty

    // cleanup
    new Directory(baseDir).deleteRecursively()
  }

  it should "list components of jars in subdirectories" in {
    // given
    val baseDirPath = Files.createTempDirectory("subdirectoriesjars")
    val baseDir = new File(baseDirPath.toUri)
    val subDirPath1 = Files.createDirectories(baseDirPath.resolve("subdir1").resolve("subdir2"))
    val subDirPath2 = Files.createDirectories(baseDirPath.resolve("subdir1").resolve("subdir3"))
    Files.createDirectories(baseDirPath.resolve("subdir4"))

    JarTestUtils.createJar(new File(subDirPath1.toUri), "jar1.jar", List(s"${DUMMYJARPATH}DummyStreamReaderOne$$.class"))
    JarTestUtils.createJar(new File(subDirPath2.toUri), "jar2.jar", List(s"${DUMMYJARPATH}DummyStreamReaderTwo$$.class"))

    // when
    val readers = ObjectScanner.getObjectsInfo(baseDir, ru.symbolOf[StreamReaderFactory]).get

    // then
    readers should contain theSameElementsAs List(
      (s"${DUMMYPACKAGE}DummyStreamReaderOne$$", subDirPath1.toAbsolutePath.toString + "/jar1.jar"),
      (s"${DUMMYPACKAGE}DummyStreamReaderTwo$$", subDirPath2.toAbsolutePath.toString + "/jar2.jar"))

    // cleanup
    new Directory(baseDir).deleteRecursively()
  }


  it should "return a failure if the given directory does not exist" in {
    // given
    val baseDirPath = Files.createTempDirectory("directorynotexist")
    val baseDir = new File(baseDirPath.toUri)
    new Directory(baseDir).delete()

    // when
    val result = ObjectScanner.getObjectsInfo(baseDir, ru.symbolOf[StreamReaderFactory])

    // then
    result.isFailure shouldBe true
    result.failed.get.getClass shouldBe classOf[IllegalArgumentException]
    result.failed.get.getMessage should fullyMatch regex "Directory .*directorynotexist.* does not exist"
  }

  it should "return a failure if the given directory is not a directory" in {
    // given
    val anyFilePath = Files.createTempFile("anyFile", ".tmp")
    val anyFile = new File(anyFilePath.toUri)

    // when
    val result = ObjectScanner.getObjectsInfo(anyFile, ru.symbolOf[StreamReaderFactory])

    // then
    result.isFailure shouldBe true
    result.failed.get.getClass shouldBe classOf[IllegalArgumentException]
    result.failed.get.getMessage should fullyMatch regex "Argument .*anyFile.*tmp is not a directory"

    // cleanup
    anyFile.delete()
  }

  it should "return a failure if a class file in any jar is invalid" in {
    // given
    val baseDirPath = Files.createTempDirectory("fakeClass")
    val baseDir = new File(baseDirPath.toUri)
    val fakeClass = new File(Files.createFile(baseDirPath.resolve("fakeClass.class")).toUri)
    JarTestUtils.createJar(baseDir, "jar1.jar", Map(fakeClass -> "fakeClass.class"))

    // when
    val result = ObjectScanner.getObjectsInfo(baseDir, ru.symbolOf[StreamReaderFactory])

    // then
    result.isFailure shouldBe true
    result.failed.get.getClass shouldBe classOf[ScalaReflectionException]
    result.failed.get.getMessage shouldBe "class fakeClass not found."

    // cleanup
    new Directory(baseDir).deleteRecursively()
  }
}

