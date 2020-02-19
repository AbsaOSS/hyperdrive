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

import java.io.{BufferedInputStream, IOException}
import java.nio.file.{Files, Path, Paths}
import java.util.jar.{Attributes, JarEntry, JarOutputStream, Manifest}

object JarTestUtils {

  val BUFFER_SIZE = 1024

  private val serviceProviderPath = "META-INF/services"

  def createJar(baseDir: Path, jarName: String, filenames: List[String], serviceProviders: Map[String, List[String]] = Map()): Path = {
    val content = filenames.map(filename => Paths.get(getClass.getClassLoader.getResource(filename).toURI) -> filename).toMap
    createJar(baseDir, jarName, content, serviceProviders)
  }

  private def createJar(baseDir: Path, jarName: String, content: Map[Path, String], serviceProviders: Map[String, List[String]]): Path = {
    val jarFile = baseDir.resolve(jarName)
    addEntries(jarFile, createManifest(), content, serviceProviders)
    jarFile
  }

  private def createManifest(): Manifest = {
    val manifest = new Manifest()
    manifest
      .getMainAttributes
      .put(Attributes.Name.MANIFEST_VERSION, "1.0")
    manifest
  }

  private def addEntries(destJarFile: Path, manifest: Manifest, content: Map[Path, String], serviceProviders: Map[String, List[String]]): Unit = {
    val outputJar = new JarOutputStream(Files.newOutputStream(destJarFile.toAbsolutePath), manifest)
    content.foreach(entry => add(entry._1, entry._2, outputJar))
    serviceProviders.foreach(entry => addServiceProvider(entry._1, entry._2, outputJar))
    outputJar.close()
  }

  @throws[IOException]
  private def add(source: Path, targetPath: String, outputJar: JarOutputStream): Unit = {
    if (Files.isDirectory(source)) {
      throw new UnsupportedOperationException("Adding directories to jars is not supported")
    }
    else {
      val entry = new JarEntry(targetPath.replace("\\", "/"))
      entry.setTime(Files.getLastModifiedTime(source).toMillis)
      outputJar.putNextEntry(entry)

      val in = new BufferedInputStream(Files.newInputStream(source))

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

  private def addServiceProvider(interface: String, providerClass: List[String], outputJar: JarOutputStream): Unit = {
    outputJar.putNextEntry(new JarEntry(s"$serviceProviderPath/$interface"))
    val providerClasses = providerClass.mkString("\n")
    outputJar.write(providerClasses.getBytes())
  }
}
