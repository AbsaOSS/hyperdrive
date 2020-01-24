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

import java.io._
import java.util.jar.{Attributes, JarEntry, JarOutputStream, Manifest}

object JarTestUtils {

  val BUFFER_SIZE = 1024

  def createJar(baseDir: File, jarName: String, filenames: List[String]): File = {
    val content = filenames.map(filename => new File(getClass.getClassLoader.getResource(filename).toURI) -> filename).toMap
    JarTestUtils.createJar(baseDir, jarName, content)
  }

  def createJar(baseDir: File, jarName: String, content: Map[File, String]): File = {
    val jarFile = new File(baseDir, jarName)
    addEntries(jarFile, createManifest(), content)
    jarFile
  }

  private def createManifest(): Manifest = {
    val manifest = new Manifest()
    manifest
      .getMainAttributes
      .put(Attributes.Name.MANIFEST_VERSION, "1.0")
    manifest
  }

  private def addEntries(destJarFile: File, manifest: Manifest, content: Map[File, String]): Unit = {
    val outputJar = new JarOutputStream(new FileOutputStream(destJarFile.getAbsolutePath), manifest)
    content.foreach(entry => add(entry._1, entry._2, outputJar))
    outputJar.close()
  }

  @throws[IOException]
  private def add(source: File, targetPath: String, outputJar: JarOutputStream): Unit = {
    if (source.isDirectory) {
      throw new UnsupportedOperationException("Adding directories to jars is not supported")
    }
    else {
      val entry = new JarEntry(targetPath.replace("\\", "/"))
      entry.setTime(source.lastModified())
      outputJar.putNextEntry(entry)

      val in = new BufferedInputStream(new FileInputStream(source))

      val buffer = new Array[Byte](BUFFER_SIZE)

      var count = in.read(buffer)
      while (count != -1) {
        outputJar.write(buffer, 0, count)
        count = in.read(buffer)
      }

      outputJar.closeEntry()
      in.close()
    }
  }
}
