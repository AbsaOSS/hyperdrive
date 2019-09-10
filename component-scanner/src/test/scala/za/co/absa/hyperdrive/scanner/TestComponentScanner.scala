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
import scala.reflect.io.Directory


class TestComponentScanner extends FlatSpec with Matchers {

  behavior of "ComponentScanner"

  val DUMMYJARPATH = "za/co/absa/hyperdrive/scanner/dummyjar/"

  it should "list API components in the same jar" in {
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
    createTestJar(filenames, baseDirPath, "jar1.jar")

    // when
    val readers = ComponentScanner.getStreamReaderComponents(baseDir).get
    val managers = ComponentScanner.getOffsetManagerComponents(baseDir).get
    val decoders = ComponentScanner.getStreamDecoderComponents(baseDir).get
    val transformers = ComponentScanner.getStreamTransformerComponents(baseDir).get
    val writers = ComponentScanner.getStreamWriterComponents(baseDir).get

    // then
    val expectedJarPath = baseDir.getAbsolutePath + "/jar1.jar"
    readers should contain only ComponentInfo("za.co.absa.hyperdrive.scanner.dummyjar.DummyStreamReaderOne$", "za.co.absa.hyperdrive.scanner.dummyjar.DummyStreamReaderOne", expectedJarPath)
    managers should contain only ComponentInfo("za.co.absa.hyperdrive.scanner.dummyjar.DummyOffsetManager$", "za.co.absa.hyperdrive.scanner.dummyjar.DummyOffsetManager", expectedJarPath)
    decoders should contain only ComponentInfo("za.co.absa.hyperdrive.scanner.dummyjar.DummyStreamDecoder$", "za.co.absa.hyperdrive.scanner.dummyjar.DummyStreamDecoder", expectedJarPath)
    transformers should contain only ComponentInfo("za.co.absa.hyperdrive.scanner.dummyjar.DummyStreamTransformer$", "za.co.absa.hyperdrive.scanner.dummyjar.DummyStreamTransformer", expectedJarPath)
    writers should contain only ComponentInfo("za.co.absa.hyperdrive.scanner.dummyjar.DummyStreamWriterOne$", "za.co.absa.hyperdrive.scanner.dummyjar.DummyStreamWriterOne", expectedJarPath)

    // cleanup
    new Directory(baseDir).deleteRecursively()
  }

  it should "list API components in multiple jars" in {
    // given
    val baseDirPath = Files.createTempDirectory("listMultipleComponentsInMultipleJars")
    val baseDir = new File(baseDirPath.toUri)
    val filesJar1 = List(
      s"${DUMMYJARPATH}DummyStreamReaderOne.class",
      s"${DUMMYJARPATH}DummyStreamReaderOne$$.class",
      s"${DUMMYJARPATH}DummyStreamWriterOne.class",
      s"${DUMMYJARPATH}DummyStreamWriterOne$$.class")
    createTestJar(filesJar1, baseDirPath, "jar1.jar")

    val filesJar2 = List(
      s"${DUMMYJARPATH}DummyStreamReaderTwo.class",
      s"${DUMMYJARPATH}DummyStreamReaderTwo$$.class",
      s"${DUMMYJARPATH}DummyStreamWriterTwo.class",
      s"${DUMMYJARPATH}DummyStreamWriterTwo$$.class")
    createTestJar(filesJar2, baseDirPath, "jar2.jar")

    // when
    val readers = ComponentScanner.getStreamReaderComponents(baseDir).get
    val writers = ComponentScanner.getStreamWriterComponents(baseDir).get

    // then
    readers should contain theSameElementsAs List(
      ComponentInfo("za.co.absa.hyperdrive.scanner.dummyjar.DummyStreamReaderOne$", "za.co.absa.hyperdrive.scanner.dummyjar.DummyStreamReaderOne", baseDir.getAbsolutePath + "/jar1.jar"),
      ComponentInfo("za.co.absa.hyperdrive.scanner.dummyjar.DummyStreamReaderTwo$", "za.co.absa.hyperdrive.scanner.dummyjar.DummyStreamReaderTwo", baseDir.getAbsolutePath + "/jar2.jar"))

    writers should contain theSameElementsAs List(
      ComponentInfo("za.co.absa.hyperdrive.scanner.dummyjar.DummyStreamWriterOne$", "za.co.absa.hyperdrive.scanner.dummyjar.DummyStreamWriterOne", baseDir.getAbsolutePath + "/jar1.jar"),
      ComponentInfo("za.co.absa.hyperdrive.scanner.dummyjar.DummyStreamWriterTwo$", "za.co.absa.hyperdrive.scanner.dummyjar.DummyStreamWriterTwo", baseDir.getAbsolutePath + "/jar2.jar"))

    // cleanup
    new Directory(baseDir).deleteRecursively()
  }

  it should "list API components of the same type in the same jar" in {
    // given
    val baseDirPath = Files.createTempDirectory("listMultipleComponentsInSingleJar")
    val baseDir = new File(baseDirPath.toUri)
    val files = List(
      s"${DUMMYJARPATH}DummyStreamReaderOne.class",
      s"${DUMMYJARPATH}DummyStreamReaderOne$$.class",
      s"${DUMMYJARPATH}DummyStreamReaderTwo.class",
      s"${DUMMYJARPATH}DummyStreamReaderTwo$$.class")
    createTestJar(files, baseDirPath, "jar1.jar")

    // when
    val readers = ComponentScanner.getStreamReaderComponents(baseDir).get

    // then
    readers should contain theSameElementsAs List(
      ComponentInfo("za.co.absa.hyperdrive.scanner.dummyjar.DummyStreamReaderOne$", "za.co.absa.hyperdrive.scanner.dummyjar.DummyStreamReaderOne", baseDir.getAbsolutePath + "/jar1.jar"),
      ComponentInfo("za.co.absa.hyperdrive.scanner.dummyjar.DummyStreamReaderTwo$", "za.co.absa.hyperdrive.scanner.dummyjar.DummyStreamReaderTwo", baseDir.getAbsolutePath + "/jar1.jar"))

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
    createTestJar(files, baseDirPath, "jar1.jar")

    // when
    val readers = ComponentScanner.getStreamReaderComponents(baseDir).get

    // then
    readers should contain theSameElementsAs List(
      ComponentInfo("za.co.absa.hyperdrive.scanner.dummyjar.DummyStreamReaderOne$", "za.co.absa.hyperdrive.scanner.dummyjar.DummyStreamReaderOne", baseDir.getAbsolutePath + "/jar1.jar"),
      ComponentInfo("za.co.absa.hyperdrive.scanner.dummyjar.DummyStreamReaderTwo$", "za.co.absa.hyperdrive.scanner.dummyjar.DummyStreamReaderTwo", baseDir.getAbsolutePath + "/jar1.jar"))

    // cleanup
    new Directory(baseDir).deleteRecursively()
  }

  it should "return a failure if the given directory does not exist" in {
    // given
    val baseDirPath = Files.createTempDirectory("directorynotexist")
    val baseDir = new File(baseDirPath.toUri)
    new Directory(baseDir).delete()

    // when
    val result = ComponentScanner.getStreamReaderComponents(baseDir)

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
    val result = ComponentScanner.getStreamReaderComponents(anyFile)

    // then
    result.isFailure shouldBe true
    result.failed.get.getClass shouldBe classOf[IllegalArgumentException]
    result.failed.get.getMessage should fullyMatch regex "Argument .*anyFile.*tmp is not a directory"
  }

  it should "return a failure if a class file in any jar is invalid" in {
    // given
    val baseDirPath = Files.createTempDirectory("fakeClass")
    val baseDir = new File(baseDirPath.toUri)
    createTestJar(List("fakeClass.class"), baseDirPath, "jar1.jar")

    // when
    val result = ComponentScanner.getStreamReaderComponents(baseDir)

    // then
    result.isFailure shouldBe true
    result.failed.get.getClass shouldBe classOf[ScalaReflectionException]
    result.failed.get.getMessage shouldBe "class fakeClass not found."
  }

  private def createTestJar(filenames: List[String], baseDir: Path, jarname: String): File = {
    val classfiles = filenames.map(filename => new File(getClass.getClassLoader.getResource(filename).toURI) -> filename).toMap
    JarTestUtils.createJar(new File(baseDir.toUri), jarname, classfiles)
  }
}

