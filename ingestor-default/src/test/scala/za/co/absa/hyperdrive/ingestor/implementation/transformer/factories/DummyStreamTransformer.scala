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

package za.co.absa.hyperdrive.ingestor.implementation.transformer.factories

import org.apache.commons.configuration2.Configuration
import org.apache.spark.sql.DataFrame
import za.co.absa.hyperdrive.ingestor.api.transformer.{StreamTransformer, StreamTransformerFactory}
import za.co.absa.hyperdrive.ingestor.api.{HasComponentAttributes, PropertyMetadata}

import scala.util.Try

class DummyStreamTransformer (val dummyProperty1: String, val dummyProperty2: Int,
                              val dummyProperty3: String, val dummyProperty4: String) extends StreamTransformer {
  override def transform(streamData: DataFrame): DataFrame = ???
}

object DummyStreamTransformer extends StreamTransformerFactory with HasComponentAttributes {
  val DummyProperty1Name = "dummy.property.one"
  val DummyProperty2Name = "dummy.property.two"
  val DummyProperty3Name = "dummy.property.three"

  override def apply(config: Configuration): StreamTransformer = {
    val dummyProperty1 = Try(config.getString(DummyProperty1Name)).getOrElse("defaultValue")
    val dummyProperty2 = Try(config.getInt(DummyProperty2Name)).getOrElse(0)
    val dummyProperty3 = Try(config.getString(DummyProperty3Name)).getOrElse("defaultValue3")
    val dummyProperty4 = Try(config.getString(GlobalConfigKeys.GlobalKey2)).getOrElse("defaultValue4")
    new DummyStreamTransformer(dummyProperty1, dummyProperty2, dummyProperty3, dummyProperty4)
  }

  override def getMappingFromRetainedGlobalConfigToLocalConfig(globalConfig: Configuration): Map[String, String] = Map(
    GlobalConfigKeys.GlobalKey1 -> DummyProperty3Name,
    GlobalConfigKeys.GlobalKey2 -> GlobalConfigKeys.GlobalKey2
  )


  override def getName: String = ???
  override def getDescription: String = ???
  override def getProperties: Map[String, PropertyMetadata] = ???
}
