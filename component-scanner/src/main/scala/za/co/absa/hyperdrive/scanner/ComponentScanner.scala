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

import za.co.absa.hyperdrive.ingestor.api.decoder.StreamDecoderFactory
import za.co.absa.hyperdrive.ingestor.api.manager.StreamManagerFactory
import za.co.absa.hyperdrive.ingestor.api.reader.StreamReaderFactory
import za.co.absa.hyperdrive.ingestor.api.transformer.StreamTransformerFactory
import za.co.absa.hyperdrive.ingestor.api.writer.StreamWriterFactory

import scala.reflect.runtime.{universe => ru}
import scala.util.Try

case class ComponentInfo(fullyQualifiedName: String,
                         humanReadableName: String,
                         jarPath: String)

object ComponentScanner {

  def getStreamReaderComponents(directory: File): Try[List[ComponentInfo]] = getComponentsInfo(directory, ru.symbolOf[StreamReaderFactory])

  def getOffsetManagerComponents(directory: File): Try[List[ComponentInfo]] = getComponentsInfo(directory, ru.symbolOf[StreamManagerFactory])

  def getStreamDecoderComponents(directory: File): Try[List[ComponentInfo]] = getComponentsInfo(directory, ru.symbolOf[StreamDecoderFactory])

  def getStreamTransformerComponents(directory: File): Try[List[ComponentInfo]] = getComponentsInfo(directory, ru.symbolOf[StreamTransformerFactory])

  def getStreamWriterComponents(directory: File): Try[List[ComponentInfo]] = getComponentsInfo(directory, ru.symbolOf[StreamWriterFactory])

  def getComponentsInfo(directory: File, factoryType: ru.TypeSymbol): Try[List[ComponentInfo]] = {
    for {
      objectsInfo <- ObjectScanner.getObjectsInfo(directory, factoryType)
      componentsInfo <- Try(objectsInfo.map{case (className, jarPath) => ComponentInfo(className, getHumanReadableName(className), jarPath)})
    } yield componentsInfo
  }

  private def getHumanReadableName(fullyQualifiedName: String) = {
    if (fullyQualifiedName.takeRight(1) == "$") fullyQualifiedName.dropRight(1) else fullyQualifiedName
  }
}
