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

import java.io.File
import java.net.URLClassLoader
import java.util.zip.ZipFile

import scala.reflect.runtime.{universe => ru}
import scala.collection.JavaConverters._

object ComponentScanner {

  def findAllJarsInDirectory(directory: File): List[File] = {
    if (!directory.exists()) throw new IllegalArgumentException(s"Argument $directory does not exist")
    if (!directory.isDirectory) throw new IllegalArgumentException(s"Argument $directory is not a directory")
    directory
      .listFiles()
      .filter(_.isFile)
      .filter(_.getName.endsWith(".jar"))
      .toList
  }

  def findClassesImplementing(typeSymbol: ru.TypeSymbol, file: File): List[String] = {
    val zipFile = new ZipFile(file.getPath)
    val classLoader = new URLClassLoader(Array(file.toURI.toURL))
    val implementing = zipFile
      .stream
      .iterator
      .asScala
      .map(_.getName)
      .filter(_.endsWith(".class"))
      .map(_.replace(".class", "").replace('/', '.'))
      .map(loadClassOfType(_, typeSymbol, classLoader))
      .filter(_.isDefined)
      .map(_.get)
      .toList
    zipFile.close()

    implementing
  }

  def loadClassOfType(fullyQualifiedName: String, superType: ru.TypeSymbol, classLoader: ClassLoader): Option[String] = {
    val mirror = ru.runtimeMirror(classLoader)
    val module = mirror.staticClass(fullyQualifiedName)
    val classSymbol = mirror.reflectClass(module).symbol
    val baseClasses = classSymbol.baseClasses
    if (baseClasses.contains(superType) && !classSymbol.isAbstract && !classSymbol.isTrait) Some(fullyQualifiedName) else None
  }
}
