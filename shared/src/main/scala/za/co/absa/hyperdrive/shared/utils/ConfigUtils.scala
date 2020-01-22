/*
 * Copyright 2018-2019 ABSA Group Limited
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

package za.co.absa.hyperdrive.shared.utils

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

  def getOrNone(key: String, configuration: Configuration): Option[String] = {
    Try(getOrThrow(key, configuration)) match {
      case Success(value) => Some(value)
      case Failure(exception) => None
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
}
