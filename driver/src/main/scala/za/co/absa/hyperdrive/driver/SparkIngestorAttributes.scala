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

package za.co.absa.hyperdrive.driver

import za.co.absa.hyperdrive.driver.TerminationMethodEnum.{AwaitTermination, ProcessAllAvailable}
import za.co.absa.hyperdrive.ingestor.api.{HasComponentAttributes, PropertyMetadata}

trait SparkIngestorAttributes extends HasComponentAttributes {

  val keysPrefix = "ingestor.spark"
  val optionalConfKey = s"$keysPrefix.option"
  val KEY_APP_NAME = s"$keysPrefix.app.name"
  val KEY_TERMINATION_METHOD = s"$keysPrefix.termination.method"

  override def getName: String = "Spark Ingestor"

  override def getDescription: String = "Component that invokes Spark for the ingestion"

  override def getProperties: Map[String, PropertyMetadata] = Map(
    KEY_APP_NAME -> PropertyMetadata("Name of Spark application", None, required = true),
    KEY_TERMINATION_METHOD -> PropertyMetadata("Termination method",
      Some(s"Either '$ProcessAllAvailable' (stop when no more messages arrive) or '$AwaitTermination' (stop on signal)." +
        s" Default is '$ProcessAllAvailable'"), required = false)
  )

  override def getExtraConfigurationPrefix: Option[String] = Some(optionalConfKey)
}
