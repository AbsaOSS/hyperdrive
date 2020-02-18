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
import java.net.URLClassLoader
import java.util.ServiceLoader
import java.util.zip.ZipFile

import org.apache.logging.log4j.LogManager
import za.co.absa.hyperdrive.ingestor.api.decoder.{StreamDecoderFactory, StreamDecoderFactoryProvider}
import za.co.absa.hyperdrive.ingestor.api.manager.{StreamManagerFactory, StreamManagerFactoryProvider}
import za.co.absa.hyperdrive.ingestor.api.reader.{StreamReaderFactory, StreamReaderFactoryProvider}
import za.co.absa.hyperdrive.ingestor.api.transformer.{StreamTransformerFactory, StreamTransformerFactoryProvider}
import za.co.absa.hyperdrive.ingestor.api.writer.{StreamWriterFactory, StreamWriterFactoryProvider}
import za.co.absa.hyperdrive.ingestor.api.{ComponentFactoryProvider, HasComponentAttributes}

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

case class ComponentDescriptors(readers: Seq[ComponentDescriptor],
                                decoders: Seq[ComponentDescriptor],
                                transformers: Seq[ComponentDescriptor],
                                writers: Seq[ComponentDescriptor],
                                managers: Seq[ComponentDescriptor])

case class ComponentDescriptor(attributes: HasComponentAttributes,
                               fullyQualifiedName: String,
                               jarPath: File)

object ComponentScanner {
  private val logger = LogManager.getLogger
  private val jarSuffix = ".jar"

  def getComponents(baseDirectory: File): Try[ComponentDescriptors] = getComponents(List(baseDirectory))

  def getComponents(baseDirectories: List[File]): Try[ComponentDescriptors] = {
    Try(
      flatten(baseDirectories
        .flatMap(findAllJarsInDirectory)
        .map(f => findComponentsInJar(f)))
    )
  }

  private def flatten(componentDescriptors: Seq[ComponentDescriptors]) = {
    ComponentDescriptors(
      componentDescriptors.flatMap(_.readers),
      componentDescriptors.flatMap(_.decoders),
      componentDescriptors.flatMap(_.transformers),
      componentDescriptors.flatMap(_.writers),
      componentDescriptors.flatMap(_.managers)
    )
  }

  private def findAllJarsInDirectory(directory: File): List[File] = {
    if (!directory.exists()) throw new IllegalArgumentException(s"Directory $directory does not exist")
    if (!directory.isDirectory) throw new IllegalArgumentException(s"Argument $directory is not a directory")
    findAllJarsInDirectoryRecursively(directory)
  }

  private def findAllJarsInDirectoryRecursively(directory: File): List[File] = {
    val jarsInSubdirectories = directory
      .listFiles()
      .filter(_.isDirectory)
      .flatMap(findAllJarsInDirectoryRecursively)

    val jars = directory
      .listFiles()
      .filter(_.isFile)
      .filter(_.getName.endsWith(jarSuffix))
      .toList

    jars ++ jarsInSubdirectories
  }

  private def findComponentsInJar(jar: File): ComponentDescriptors = {
    Try(new ZipFile(jar.getPath)) match {
      case Failure(exception) =>
        logger.warn(s"Could not open file ${jar.getPath}", exception)
        ComponentDescriptors(List(), List(), List(), List(), List())
      case Success(zipFile) =>
        zipFile.close()
        val classLoader = new URLClassLoader(Array(jar.toURI.toURL))

        ComponentDescriptors(
          loadService[StreamReaderFactoryProvider, StreamReaderFactory](classLoader, jar),
          loadService[StreamDecoderFactoryProvider, StreamDecoderFactory](classLoader, jar),
          loadService[StreamTransformerFactoryProvider, StreamTransformerFactory](classLoader, jar),
          loadService[StreamWriterFactoryProvider, StreamWriterFactory](classLoader, jar),
          loadService[StreamManagerFactoryProvider, StreamManagerFactory](classLoader, jar)
        )
    }
  }

  private def loadService[P <: ComponentFactoryProvider[F], F <: HasComponentAttributes](classLoader: ClassLoader, jar: File)(implicit classTag: ClassTag[P]): List[ComponentDescriptor] = {
    loadService[P, F](classLoader)
      .map(factory => ComponentDescriptor(factory, factory.getClass.getName, jar))
      .toList
  }

  def loadService[P <: ComponentFactoryProvider[F], F <: HasComponentAttributes](classLoader: ClassLoader)(implicit classTag: ClassTag[P]): Iterable[F] = {
    import scala.collection.JavaConverters._
    ServiceLoader.load(classTag.runtimeClass, classLoader)
      .asScala
      .map(c => c.asInstanceOf[P])
      .map(c => c.getComponentFactory)
  }
}
