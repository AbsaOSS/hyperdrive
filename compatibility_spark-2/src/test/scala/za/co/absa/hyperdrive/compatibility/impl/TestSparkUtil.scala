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

package za.co.absa.hyperdrive.compatibility.impl

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.spark.commons.test.SparkTestBase

import za.co.absa.commons.io.{TempDirectory, TempFile}

class TestSparkUtil extends AnyFlatSpec with Matchers with SparkTestBase {

  "createMetadataLogFileIndex" should "return a metadata log file index" in {
    val result = SparkUtil.createMetadataLogFileIndex(
      spark,
      TempDirectory("createMetadataLogFileIndex").path.toAbsolutePath.toString,
      None
    )

    result shouldBe a[org.apache.spark.sql.execution.streaming.MetadataLogFileIndex]
  }

  "hasMetadata" should "return true if metadata exists" in {
    val result = SparkUtil.hasMetadata(
      spark,
      TempFile("hasMetadata").path.toAbsolutePath.toString
    )

    result shouldBe false
  }

  "jsonStringToObject" should "return an object from a JSON string" in {
    val jsonString = """{"key": "value"}"""

    val result = SparkUtil.jsonStringToObject(jsonString)

    import scala.collection.JavaConverters._
    result.asInstanceOf[java.util.Map[String, String]].asScala should contain("key" -> "value")
  }

  "objectToJsonString" should "return a JSON string from an object" in {
    import scala.collection.JavaConverters._
    val obj = Map("key" -> "value").asJava

    val result = SparkUtil.objectToJsonString(obj)

    result shouldBe defined
    result.get shouldBe """{"key":"value"}"""
  }
}
