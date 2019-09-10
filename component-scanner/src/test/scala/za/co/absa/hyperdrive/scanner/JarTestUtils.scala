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

package za.co.absa.hyperdrive.scanner

import java.io._
import java.util.jar.{Attributes, JarEntry, JarOutputStream, Manifest}

object JarTestUtils {

  def createJar(baseDir: File, jarName: String, content: List[File]): File = {
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

  private def addEntries(destJarFile: File, manifest: Manifest, content: List[File]): Unit = {
    val target = new JarOutputStream(new FileOutputStream(destJarFile.getAbsolutePath), manifest)
    content.foreach(entry => add(entry, target))
  }

  @throws[IOException]
  private def add(source: File, target: JarOutputStream): Unit = {
    if (source.isDirectory) {
      var name = source.getPath.replace("\\", "/")
      if (!name.isEmpty) {
        if (!name.endsWith("/")) {
          name += "/"
        }

        val entry = new JarEntry(name)
        entry.setTime(source.lastModified())

        target.putNextEntry(entry)
        target.closeEntry()
      }

      source.listFiles().foreach(file => add(file, target))
    }
    else {
      val entry = new JarEntry(source.getPath.replace("\\", "/"))
      entry.setTime(source.lastModified())
      target.putNextEntry(entry)

      val in = new BufferedInputStream(new FileInputStream(source))

      val buffer = new Array[Byte](1024)

      var count = in.read(buffer)
      while (count != -1) {
        target.write(buffer, 0, count)
        count = in.read(buffer)
      }

      target.closeEntry()
      in.close()
    }
  }
}
