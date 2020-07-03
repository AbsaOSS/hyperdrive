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

import java.net.URLClassLoader
import java.nio.file.{Files, Path}
import java.util.ServiceLoader

import org.apache.logging.log4j.LogManager
import za.co.absa.hyperdrive.ingestor.api.reader.{StreamReaderFactory, StreamReaderFactoryProvider}
import za.co.absa.hyperdrive.ingestor.api.transformer.{StreamTransformerFactory, StreamTransformerFactoryProvider}
import za.co.absa.hyperdrive.ingestor.api.writer.{StreamWriterFactory, StreamWriterFactoryProvider}
import za.co.absa.hyperdrive.ingestor.api.{ComponentFactory, ComponentFactoryProvider, HasComponentAttributes}

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

case class ComponentDescriptors(readers: Seq[ComponentDescriptor],
                                transformers: Seq[ComponentDescriptor],
                                writers: Seq[ComponentDescriptor])

case class ComponentDescriptor(attributes: HasComponentAttributes,
                               fullyQualifiedName: String,
                               jarPath: Path)

object ComponentScanner {
  private val logger = LogManager.getLogger
  private val jarSuffix = ".jar"

  def getComponents(baseDirectory: Path): Try[ComponentDescriptors] = getComponents(List(baseDirectory))

  def getComponents(baseDirectories: List[Path]): Try[ComponentDescriptors] = {
    Try(
      flatten(baseDirectories
        .flatMap(findJarsInDirectory)
        .map(jar => findComponentsInJar(jar)))
    )
  }

  private def flatten(componentDescriptors: Seq[ComponentDescriptors]) = {
    ComponentDescriptors(
      componentDescriptors.flatMap(_.readers),
      componentDescriptors.flatMap(_.transformers),
      componentDescriptors.flatMap(_.writers)
    )
  }

  private def findJarsInDirectory(directory: Path): List[Path] = {
    if (!Files.exists(directory)) {
      throw new IllegalArgumentException(s"Directory $directory does not exist")
    }
    if (!Files.isDirectory(directory)) {
      throw new IllegalArgumentException(s"Argument $directory is not a directory")
    }

    import scala.collection.JavaConverters._
    Files.list(directory)
      .iterator()
      .asScala
      .filter(p => Files.isRegularFile(p))
      .filter(p => p.toString.endsWith(jarSuffix))
      .toList
  }

  private def findComponentsInJar(jar: Path): ComponentDescriptors = {
    val classLoader = new URLClassLoader(Array(jar.toUri.toURL))
    ComponentDescriptors(
      loadService[StreamReaderFactoryProvider, StreamReaderFactory](classLoader, jar),
      loadService[StreamTransformerFactoryProvider, StreamTransformerFactory](classLoader, jar),
      loadService[StreamWriterFactoryProvider, StreamWriterFactory](classLoader, jar)
    )
  }

  private def loadService[P <: ComponentFactoryProvider[F], F <: ComponentFactory[_]](classLoader: ClassLoader, jar: Path)(implicit classTag: ClassTag[P]): List[ComponentDescriptor] = {
    import scala.collection.JavaConverters._
    Try(ServiceLoader.load(classTag.runtimeClass, classLoader)
      .asScala
      .map(untypedClass => untypedClass.asInstanceOf[P])
      .map(provider => provider.getComponentFactory)
      .map(factory => ComponentDescriptor(factory, factory.getClass.getName, jar))
      .toList
    ) match {
      case Failure(exception) =>
        logger.warn(s"Could not load components from ${jar.toAbsolutePath}", exception)
        List()
      case Success(components) => components
    }
  }
}
