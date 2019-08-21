package za.co.absa.hyperdrive.shared.test.utils

import java.io._
import java.nio.file.Files
import java.util.jar.Attributes
import java.util.jar.JarEntry
import java.util.jar.JarOutputStream
import java.util.jar.Manifest

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
