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

package za.co.absa.hyperdrive.compatibility.impl.writer.delta

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.scalatest.{BeforeAndAfterEach, FlatSpec}
import za.co.absa.commons.io.TempDirectory
import za.co.absa.hyperdrive.shared.utils.SparkTestBase

import scala.reflect.io.Path

trait DeltaTestBase extends BeforeAndAfterEach with SparkTestBase {
  this: FlatSpec =>

  val baseDir: TempDirectory = TempDirectory("DeltaTempDir").deleteOnExit()
  val destinationPath = s"${baseDir.path.toAbsolutePath.toString}/destination"
  val checkpointPath = s"${baseDir.path.toAbsolutePath.toString}/checkpoint"

  import spark.implicits._

  val memoryStream: MemoryStream[CDCEvent] = MemoryStream[CDCEvent](1, spark.sqlContext)

  override def beforeEach(): Unit = {
    Path(destinationPath).deleteRecursively()
    Path(checkpointPath).deleteRecursively()
    memoryStream.reset()
  }

  def getResult: Seq[CDCEvent] = {
    import spark.implicits._
    spark.read.format("delta").load(destinationPath).as[CDCEvent].collect().toSeq
  }
}
