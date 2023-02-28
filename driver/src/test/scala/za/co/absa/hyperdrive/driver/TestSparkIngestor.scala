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

import org.apache.commons.configuration2.BaseConfiguration
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mockito.ArgumentMatchers.{eq => eqTo, _}
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.hyperdrive.ingestor.api.reader.StreamReader
import za.co.absa.hyperdrive.ingestor.api.transformer.StreamTransformer
import za.co.absa.hyperdrive.ingestor.api.writer.StreamWriter
import za.co.absa.hyperdrive.shared.exceptions.{IngestionException, IngestionStartException}
import za.co.absa.spark.commons.test.SparkTestBase

class TestSparkIngestor extends AnyFlatSpec with BeforeAndAfterEach with MockitoSugar with SparkTestBase with Matchers {

  private val streamReader: StreamReader = mock[StreamReader]
  private val streamTransformer: StreamTransformer = mock[StreamTransformer]
  private val streamWriter: StreamWriter = mock[StreamWriter]

  private val dataFrame: DataFrame = mock[DataFrame]
  private val streamingQuery: StreamingQuery = mock[StreamingQuery]
  private val configuration = new BaseConfiguration

  override def beforeEach(): Unit = {
    // Used to initialize spark session in SparkTestBase trait
    spark.sparkContext
    reset(
      streamReader,
      streamTransformer,
      streamWriter,
      dataFrame,
      streamingQuery)
  }

  behavior of SparkIngestor.getClass.getName

  it should "throw IngestionStartException if stream reader fails during setup" in {
    val sparkIngestor = SparkIngestor(configuration)
    when(streamReader.read(any[SparkSession])).thenThrow(classOf[RuntimeException])
    assertThrows[IngestionStartException](sparkIngestor.ingest(streamReader, Seq(streamTransformer), streamWriter))
  }

  it should "throw IngestionStartException if stream transformer fails during setup" in {
    val sparkIngestor = SparkIngestor(configuration)
    when(streamReader.read(any[SparkSession])).thenReturn(dataFrame)
    when(streamTransformer.transform(dataFrame)).thenThrow(classOf[RuntimeException])
    assertThrows[IngestionStartException](sparkIngestor.ingest(streamReader, Seq(streamTransformer), streamWriter))
  }

  it should "throw IngestionStartException if stream writer fails during setup" in {
    val sparkIngestor = SparkIngestor(configuration)
    when(streamReader.read(any[SparkSession])).thenReturn(dataFrame)
    when(streamTransformer.transform(dataFrame)).thenReturn(dataFrame)
    when(streamWriter.write(dataFrame)).thenThrow(classOf[RuntimeException])
    assertThrows[IngestionStartException](sparkIngestor.ingest(streamReader, Seq(streamTransformer), streamWriter))
  }

  it should "throw IngestionException if ingestion fails during execution" in {
    val sparkIngestor = SparkIngestor(configuration)
    when(streamReader.read(any[SparkSession])).thenReturn(dataFrame)
    when(streamTransformer.transform(dataFrame)).thenReturn(dataFrame)
    when(streamWriter.write(dataFrame)).thenReturn(streamingQuery)
    when(streamingQuery.awaitTermination()).thenThrow(classOf[RuntimeException])
    assertThrows[IngestionException](sparkIngestor.ingest(streamReader, Seq(streamTransformer), streamWriter))
  }

  it should "invoke the components in correct order" in {
    val sparkIngestor = SparkIngestor(configuration)
    when(streamReader.read(any[SparkSession])).thenReturn(dataFrame)
    when(streamTransformer.transform(dataFrame)).thenReturn(dataFrame)
    when(streamWriter.write(dataFrame)).thenReturn(streamingQuery)

    sparkIngestor.ingest(streamReader, Seq(streamTransformer), streamWriter)

    val inOrderCheck = Mockito.inOrder(streamReader, streamTransformer, streamWriter)
    inOrderCheck.verify(streamReader).read(any[SparkSession])
    inOrderCheck.verify(streamTransformer).transform(dataFrame)
    inOrderCheck.verify(streamWriter).write(dataFrame)
    verify(streamingQuery).awaitTermination()
  }

  it should "use terminationMethod awaitTermination if configured" in {
    val config = new BaseConfiguration
    val sparkIngestor = SparkIngestor(config)
    when(streamReader.read(any[SparkSession])).thenReturn(dataFrame)
    when(streamTransformer.transform(dataFrame)).thenReturn(dataFrame)
    when(streamWriter.write(dataFrame)).thenReturn(streamingQuery)

    sparkIngestor.ingest(streamReader, Seq(streamTransformer), streamWriter)

    verify(streamingQuery).awaitTermination()
  }

  it should "use timeout if configured with terminationMethod awaitTermination" in {
    val config = new BaseConfiguration
    config.addProperty(s"${SparkIngestor.KEY_AWAIT_TERMINATION_TIMEOUT}", "10000")
    val sparkIngestor = SparkIngestor(config)
    when(streamReader.read(any[SparkSession])).thenReturn(dataFrame)
    when(streamTransformer.transform(dataFrame)).thenReturn(dataFrame)
    when(streamWriter.write(dataFrame)).thenReturn(streamingQuery)

    sparkIngestor.ingest(streamReader, Seq(streamTransformer), streamWriter)

    verify(streamingQuery).awaitTermination(eqTo(10000L))
  }

  it should "throw if an invalid terminationMethod is configured" in {
    val config = new BaseConfiguration
    config.addProperty(s"${SparkIngestor.KEY_TERMINATION_METHOD}", "non-existent")
    val throwable = intercept[IllegalArgumentException](SparkIngestor(config))

    throwable.getMessage should include(SparkIngestor.KEY_TERMINATION_METHOD)
  }

  it should "throw if a timeout is not a number" in {
    val config = new BaseConfiguration
    config.addProperty(s"${SparkIngestor.KEY_AWAIT_TERMINATION_TIMEOUT}", "nan")
    val throwable = intercept[IllegalArgumentException](SparkIngestor(config))

    throwable.getMessage should include(SparkIngestor.KEY_AWAIT_TERMINATION_TIMEOUT)
  }

}
