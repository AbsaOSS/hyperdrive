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

package za.co.absa.hyperdrive

import java.util.UUID

import org.apache.commons.configuration2.Configuration
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.SparkSession
import za.co.absa.hyperdrive.driver.TerminationMethodEnum.{AwaitTermination, ProcessAllAvailable, TerminationMethod}
import za.co.absa.hyperdrive.ingestor.api.decoder.StreamDecoder
import za.co.absa.hyperdrive.ingestor.api.manager.StreamManager
import za.co.absa.hyperdrive.ingestor.api.reader.StreamReader
import za.co.absa.hyperdrive.ingestor.api.transformer.StreamTransformer
import za.co.absa.hyperdrive.ingestor.api.utils.ComponentFactoryUtil
import za.co.absa.hyperdrive.ingestor.api.writer.StreamWriter
import za.co.absa.hyperdrive.shared.exceptions.{IngestionException, IngestionStartException}
import za.co.absa.hyperdrive.ingestor.api.utils.ConfigUtils

import scala.util.{Failure, Success}
import scala.util.control.NonFatal

/**
  * This object is only a placeholder to make sure that a sources jar and a javadoc jar will be generated for this module.
  * This is a requirement of maven central. The purpose of this module is to have a separate module for the fat-jar
  * such that users who want to add a dependency on the driver module are not forced to have the fat-jar in their dependencies.
  */
object SourceJarPlaceholder
