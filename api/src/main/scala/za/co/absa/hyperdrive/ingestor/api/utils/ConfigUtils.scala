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

package za.co.absa.hyperdrive.ingestor.api.utils

import org.apache.commons.configuration2.Configuration

import scala.util.{Failure, Success, Try}

object ConfigUtils {

  def getOrThrow(key: String, configuration: Configuration, errorMessage: String = ""): String = {
    configuration.getString(key) match {
      case value: String => value
      case _ =>
        val resolvedMessage = if (errorMessage.isEmpty) s"No configuration property found for key $key" else errorMessage
        throw new IllegalArgumentException(resolvedMessage)
    }
  }

  def getSeqOrThrow(key: String, configuration: Configuration, errorMessage: String = ""): Seq[String] = {
    configuration.getStringArray(key) match {
      case value if value.nonEmpty => value
      case _ =>
        val resolvedMessage = if (errorMessage.isEmpty) s"No configuration property found for key $key" else errorMessage
        throw new IllegalArgumentException(resolvedMessage)
    }
  }

  def getSeqOrNone(key: String, configuration: Configuration): Option[Seq[String]] = {
    Try(getSeqOrThrow(key, configuration)) match {
      case Success(value) => Some(value)
      case Failure(_) => None
    }
  }

  def getOrNone(key: String, configuration: Configuration): Option[String] = {
    Try(getOrThrow(key, configuration)) match {
      case Success(value) => Some(value)
      case Failure(_) => None
    }
  }

  def getBooleanOrNone(key: String, configuration: Configuration): Option[Boolean] = {
    if(configuration.containsKey(key) && configuration.getString(key).isEmpty) {
      Some(true)
    } else if (!configuration.containsKey(key)) {
      None
    } else {
      Option(configuration.getBoolean(key))
    }
  }

  def getPropertySubset(configuration: Configuration, prefix: String): Map[String, String] = {
    val subset = Option(configuration.subset(prefix))
    subset match {
      case Some(subset) =>
        import scala.collection.JavaConverters._
        val keys = subset.getKeys()
        keys.asScala
          .map(key => (key, getOrThrow(key, subset)))
          .toMap
      case _ => Map()
    }
  }

  /**
   * Copies properties defined in sourceToTargetMapping from source to target. If source config keys don't exist, the
   * method returns a Failure. If the specified target keys already exist, the method returns a Failure.
   * @param source config to copy properties from. Won't be mutated by this method.
   * @param target config to copy properties into. This method mutates the target configuration.
   * @param sourceToTargetMapping mapping from source config keys to target config keys
   * @return target configuration, wrapped in Try
   */
  def copyAndMapConfig(source: Configuration, target: Configuration, sourceToTargetMapping: Map[String, String]): Try[Configuration] = {
    def missingSourceKeys = sourceToTargetMapping.keys.filterNot(source.containsKey)
    def conflictingTargetKeys = sourceToTargetMapping.values.filter(target.containsKey)

    if (missingSourceKeys.nonEmpty) {
      Failure(new IllegalArgumentException(s"Keys $missingSourceKeys don't exist in the source configuration."))
    } else if (conflictingTargetKeys.nonEmpty) {
      Failure(new IllegalArgumentException(s"Cannot add key $conflictingTargetKeys to target configuration because they already exist."))
    } else {
      sourceToTargetMapping.foreach { case (sourceKey, targetKey) =>
        val value = source.getProperty(sourceKey)
        target.addProperty(targetKey, value)
      }
      Success(target)
    }
  }
}
