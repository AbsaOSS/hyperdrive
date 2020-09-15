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

package za.co.absa.hyperdrive.ingestor.implementation.reader.parquet

import za.co.absa.hyperdrive.ingestor.api.{HasComponentAttributes, PropertyMetadata}

trait ParquetStreamReaderAttributes extends HasComponentAttributes {
  private val rootFactoryConfKey = "reader.parquet"
  val KeySourceDirectory = s"${rootFactoryConfKey}.source.directory"

  override def getName: String = "Parquet Stream Reader"

  override def getDescription: String = "This Reader reads Parquet files from the given directory on a filesystem (e.g. HDFS)"

  override def getProperties: Map[String, PropertyMetadata] = Map(
    KeySourceDirectory -> PropertyMetadata("Source directory", Some("A path to a directory"), required = true)
  )

  override def getExtraConfigurationPrefix: Option[String] = Some(s"${rootFactoryConfKey}.options")
}
