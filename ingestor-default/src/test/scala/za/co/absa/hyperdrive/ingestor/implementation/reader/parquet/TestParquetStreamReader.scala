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

import java.nio.file.Path
import org.apache.spark.sql.SaveMode
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.commons.io.TempDirectory
import za.co.absa.spark.commons.test.SparkTestBase

class TestParquetStreamReader extends AnyFlatSpec with MockitoSugar with Matchers with SparkTestBase with BeforeAndAfterEach {
  behavior of "ParquetStreamReader"

  private var tempDir: TempDirectory = _
  private var parquetSource: Path = _
  private var parquetSourcePath: String = _

  override def beforeEach: Unit = {
    tempDir = TempDirectory().deleteOnExit()
    parquetSource = tempDir.path.resolve("source")
    parquetSourcePath = parquetSource.toAbsolutePath.toString
  }

  override def afterEach: Unit = tempDir.delete()

  it should "read from the source directory when the parquet file is already present" in {
    // given
    import spark.implicits._
    val columnName = "dummy"
    val queryName = "dummyQuery"
    val sourceDf = spark.sparkContext.parallelize(1 to 100).toDF(columnName)
    sourceDf
      .write
      .mode(SaveMode.Overwrite)
      .parquet(parquetSourcePath)

    // when
    val reader = new ParquetStreamReader(parquetSourcePath, Map())
    val streamingDf = reader.read(spark)
    val query = streamingDf
      .writeStream
      .queryName(queryName)
      .format("memory")
      .start()
    query.processAllAvailable()

    val sourceDf2 = spark.sparkContext.parallelize(101 to 200).toDF(columnName)
    sourceDf2
      .write
      .mode(SaveMode.Append)
      .parquet(parquetSourcePath)
    query.processAllAvailable()
    query.stop()

    // then
    import spark.implicits._
    val expectedData = 1 to 200
    val outputDf = spark.sql(s"select * from $queryName")
    outputDf.count() shouldBe expectedData.length
    outputDf.columns should contain theSameElementsAs List(columnName)
    outputDf.select(columnName)
      .map(_ (0).asInstanceOf[Int]).collect() should contain theSameElementsAs expectedData
  }
}
