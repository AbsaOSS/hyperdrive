/*
 *
 * Copyright 2019 ABSA Group Limited
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package za.co.absa.hyperdrive.transformations.encoding.schema.impl

import java.io.File
import java.nio.file.{Files, Paths}

import za.co.absa.hyperdrive.settings.InfrastructureSettings._
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.hyperdrive.transformations.encoding.schema.SchemaPathProvider

class SchemaRegistrySchemaPathProvider(settings: Map[String,String]) extends SchemaPathProvider {

  override def get: String = {
    val schema = AvroSchemaUtils.load(settings)
    val destinationSchemaFile = getDestinationFile(schema.getName)
    val schemaFile = Files.write(destinationSchemaFile.toPath, schema.toString(true).getBytes())
    schemaFile.toAbsolutePath.toString
  }

  private def getDestinationFile(name: String): File = {
    val baseDir = new File(HyperdriveSettings.BASE_TEMP_DIR)
    if (!baseDir.exists()) {
      baseDir.mkdirs()
    }
    val destinationFile = new File(baseDir, name)
    destinationFile.deleteOnExit()
    destinationFile
  }
}
