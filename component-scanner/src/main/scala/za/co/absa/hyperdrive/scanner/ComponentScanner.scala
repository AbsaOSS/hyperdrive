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

import za.co.absa.hyperdrive.ingestor.api.decoder.StreamDecoderFactory
import za.co.absa.hyperdrive.ingestor.api.manager.OffsetManagerFactory
import za.co.absa.hyperdrive.ingestor.api.reader.StreamReaderFactory
import za.co.absa.hyperdrive.ingestor.api.transformer.StreamTransformerFactory
import za.co.absa.hyperdrive.ingestor.api.writer.StreamWriterFactory

import scala.reflect.runtime.{universe => ru}
import scala.collection.JavaConverters._
import scala.util.Try

case class ComponentInfo(fullyQualifiedName: String,
                         humanReadableName: String,
                         jarPath: String)

object ComponentScanner {

  def getStreamReaderComponents(directory: File): Try[List[ComponentInfo]] = getComponentInfo(directory, ru.symbolOf[StreamReaderFactory])

  def getOffsetManagerComponents(directory: File): Try[List[ComponentInfo]] = getComponentInfo(directory, ru.symbolOf[OffsetManagerFactory])

  def getStreamDecoderComponents(directory: File): Try[List[ComponentInfo]] = getComponentInfo(directory, ru.symbolOf[StreamDecoderFactory])

  def getStreamTransformerComponents(directory: File): Try[List[ComponentInfo]] = getComponentInfo(directory, ru.symbolOf[StreamTransformerFactory])

  def getStreamWriterComponents(directory: File): Try[List[ComponentInfo]] = getComponentInfo(directory, ru.symbolOf[StreamWriterFactory])

  private def getComponentInfo(directory: File, factoryType: ru.TypeSymbol): Try[List[ComponentInfo]] = {
    Try(findAllJarsInDirectory(directory)
      .flatMap(findComponentClasses(factoryType, _)))
  }

  private def findAllJarsInDirectory(directory: File): List[File] = {
    if (!directory.exists()) throw new IllegalArgumentException(s"Directory $directory does not exist")
    if (!directory.isDirectory) throw new IllegalArgumentException(s"Argument $directory is not a directory")
    directory
      .listFiles()
      .filter(_.isFile)
      .filter(_.getName.endsWith(".jar"))
      .toList
  }

  private def findComponentClasses(factoryType: ru.TypeSymbol, file: File): List[ComponentInfo] = {
    val zipFile = new ZipFile(file.getPath)
    val classLoader = new URLClassLoader(Array(file.toURI.toURL))
    val components = zipFile
      .stream
      .iterator
      .asScala
      .map(_.getName)
      .filter(_.endsWith(".class"))
      .map(_.replace(".class", "").replace('/', '.'))
      .map(loadClassOfType(_, factoryType, classLoader))
      .filter(_.isDefined)
      .map(_.get)
      .map(className => ComponentInfo(className, getHumanReadableName(className), file.getAbsolutePath))
      .toList
    zipFile.close()

    components
  }

  private def getHumanReadableName(fullyQualifiedName: String) = {
    if (fullyQualifiedName.takeRight(1) == "$") fullyQualifiedName.dropRight(1) else fullyQualifiedName
  }

  private def loadClassOfType(fullyQualifiedName: String, factoryType: ru.TypeSymbol, classLoader: ClassLoader): Option[String] = {
    val mirror = ru.runtimeMirror(classLoader)
    val module = mirror.staticClass(fullyQualifiedName)
    val classSymbol = mirror.reflectClass(module).symbol
    val baseClasses = classSymbol.baseClasses
    if (baseClasses.contains(factoryType) && !classSymbol.isAbstract && !classSymbol.isTrait) Some(fullyQualifiedName) else None
  }
}
