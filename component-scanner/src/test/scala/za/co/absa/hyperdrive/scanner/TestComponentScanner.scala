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

import scala.reflect.io.Directory


class TestComponentScanner extends FlatSpec with Matchers {

  behavior of "ComponentScanner"

  val DUMMYJARPATH = "za/co/absa/hyperdrive/scanner/dummyjar/"
  val DUMMYPACKAGE = "za.co.absa.hyperdrive.scanner.dummyjar."

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
    JarTestUtils.createJar(baseDir, "jar1.jar", filenames)

    // when
    val readers = ComponentScanner.getStreamReaderComponents(baseDir).get
    val managers = ComponentScanner.getOffsetManagerComponents(baseDir).get
    val decoders = ComponentScanner.getStreamDecoderComponents(baseDir).get
    val transformers = ComponentScanner.getStreamTransformerComponents(baseDir).get
    val writers = ComponentScanner.getStreamWriterComponents(baseDir).get

    // then
    val expectedJarPath = baseDir.getAbsolutePath + "/jar1.jar"
    readers should contain only ComponentInfo(s"${DUMMYPACKAGE}DummyStreamReaderOne$$", s"${DUMMYPACKAGE}DummyStreamReaderOne", expectedJarPath)
    managers should contain only ComponentInfo(s"${DUMMYPACKAGE}DummyOffsetManager$$", s"${DUMMYPACKAGE}DummyOffsetManager", expectedJarPath)
    decoders should contain only ComponentInfo(s"${DUMMYPACKAGE}DummyStreamDecoder$$", s"${DUMMYPACKAGE}DummyStreamDecoder", expectedJarPath)
    transformers should contain only ComponentInfo(s"${DUMMYPACKAGE}DummyStreamTransformer$$", s"${DUMMYPACKAGE}DummyStreamTransformer", expectedJarPath)
    writers should contain only ComponentInfo(s"${DUMMYPACKAGE}DummyStreamWriterOne$$", s"${DUMMYPACKAGE}DummyStreamWriterOne", expectedJarPath)

    // cleanup
    new Directory(baseDir).deleteRecursively()
  }
}

