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

package za.co.absa.hyperdrive.driver.utils

import java.util.Properties

import scala.util.Try


object DriverUtil {

  private val VERSIONS_FILE = "version.properties"
  private val PROJECT_VERSION_KEY = "project.version"
  private val IMPLEMENTATION_VERSION_KEY = "implementation.version"
  private val UNKNOWN = "<unknown>"

  def getVersionString: String = {
    val versions = getVersions
    s"Version: ${versions._1 getOrElse UNKNOWN}, Implementation-Version: ${versions._2 getOrElse UNKNOWN}"
  }

  private def getVersions: (Option[String], Option[String]) = {
    Try {
      val properties = new Properties()
      properties.load(getClass.getClassLoader.getResourceAsStream(VERSIONS_FILE))
      (Option(properties.getProperty(PROJECT_VERSION_KEY)), Option(properties.getProperty(IMPLEMENTATION_VERSION_KEY)))
    } getOrElse ((None, None))
  }
}
