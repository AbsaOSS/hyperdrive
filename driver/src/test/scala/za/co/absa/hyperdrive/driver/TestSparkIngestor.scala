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
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.streaming.{DataStreamReader, StreamingQuery}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mockito.Mockito
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers.{eq => eqTo}
import org.mockito.ArgumentMatchers._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import za.co.absa.hyperdrive.driver.TerminationMethodEnum.AwaitTermination
import za.co.absa.hyperdrive.ingestor.api.decoder.StreamDecoder
import za.co.absa.hyperdrive.ingestor.api.manager.StreamManager
import za.co.absa.hyperdrive.ingestor.api.reader.StreamReader
import za.co.absa.hyperdrive.ingestor.api.transformer.StreamTransformer
import za.co.absa.hyperdrive.ingestor.api.writer.StreamWriter
import za.co.absa.hyperdrive.shared.exceptions.{IngestionException, IngestionStartException}
import za.co.absa.commons.spark.SparkTestBase

class TestSparkIngestor extends FlatSpec with BeforeAndAfterEach with MockitoSugar with SparkTestBase with Matchers {

  private val streamReader: StreamReader = mock[StreamReader]
  private val streamManager: StreamManager = mock[StreamManager]
  private val streamDecoder: StreamDecoder = mock[StreamDecoder]
  private val streamTransformer: StreamTransformer = mock[StreamTransformer]
  private val streamWriter: StreamWriter = mock[StreamWriter]

  private val dataStreamReaderMock: DataStreamReader = mock[DataStreamReader]
  private val dataFrame: DataFrame = mock[DataFrame]
  private val streamingQuery: StreamingQuery = mock[StreamingQuery]
  private val configuration = {
    val config = new BaseConfiguration
    config.addProperty(SparkIngestor.KEY_APP_NAME, "my-app-name")
    config
  }

  override def beforeEach(): Unit = {
    reset(
      streamReader,
      streamManager,
      streamDecoder,
      streamTransformer,
      streamWriter,
      dataFrame,
      streamingQuery)
  }

  behavior of SparkIngestor.getClass.getName

  it should "throw IngestionStartException if stream reader fails during setup" in {
    val sparkIngestor = SparkIngestor(configuration)
    when(streamReader.read(any[SparkSession])).thenThrow(classOf[RuntimeException])
    assertThrows[IngestionStartException](sparkIngestor.ingest(streamReader, streamManager, streamDecoder, streamTransformer, streamWriter))
  }

  it should "throw IngestionStartException if stream manager fails during setup" in {
    val sparkIngestor = SparkIngestor(configuration)
    when(streamReader.read(any[SparkSession])).thenReturn(dataStreamReaderMock)
    when(streamManager.configure(eqTo(dataStreamReaderMock), any[Configuration])).thenThrow(classOf[RuntimeException])
    assertThrows[IngestionStartException](sparkIngestor.ingest(streamReader, streamManager, streamDecoder, streamTransformer, streamWriter))
  }

  it should "throw IngestionStartException if format decoder fails during setup" in {
    val sparkIngestor = SparkIngestor(configuration)
    when(streamReader.read(any[SparkSession])).thenReturn(dataStreamReaderMock)
    when(streamManager.configure(eqTo(dataStreamReaderMock), any[Configuration])).thenReturn(dataStreamReaderMock)
    when(streamDecoder.decode(dataStreamReaderMock)).thenThrow(classOf[RuntimeException])
    assertThrows[IngestionStartException](sparkIngestor.ingest(streamReader, streamManager, streamDecoder, streamTransformer, streamWriter))
  }

  it should "throw IngestionStartException if stream transformer fails during setup" in {
    val sparkIngestor = SparkIngestor(configuration)
    when(streamReader.read(any[SparkSession])).thenReturn(dataStreamReaderMock)
    when(streamManager.configure(eqTo(dataStreamReaderMock), any[Configuration])).thenReturn(dataStreamReaderMock)
    when(streamDecoder.decode(dataStreamReaderMock)).thenReturn(dataFrame)
    when(streamTransformer.transform(dataFrame)).thenThrow(classOf[RuntimeException])
    assertThrows[IngestionStartException](sparkIngestor.ingest(streamReader, streamManager, streamDecoder, streamTransformer, streamWriter))
  }

  it should "throw IngestionStartException if stream writer fails during setup" in {
    val sparkIngestor = SparkIngestor(configuration)
    when(streamReader.read(any[SparkSession])).thenReturn(dataStreamReaderMock)
    when(streamManager.configure(eqTo(dataStreamReaderMock), any[Configuration])).thenReturn(dataStreamReaderMock)
    when(streamDecoder.decode(dataStreamReaderMock)).thenReturn(dataFrame)
    when(streamTransformer.transform(dataFrame)).thenReturn(dataFrame)
    when(streamWriter.write(dataFrame, streamManager)).thenThrow(classOf[RuntimeException])
    assertThrows[IngestionStartException](sparkIngestor.ingest(streamReader, streamManager, streamDecoder, streamTransformer, streamWriter))
  }

  it should "throw IngestionException if ingestion fails during execution" in {
    val sparkIngestor = SparkIngestor(configuration)
    when(streamReader.read(any[SparkSession])).thenReturn(dataStreamReaderMock)
    when(streamManager.configure(eqTo(dataStreamReaderMock), any[Configuration])).thenReturn(dataStreamReaderMock)
    when(streamDecoder.decode(dataStreamReaderMock)).thenReturn(dataFrame)
    when(streamTransformer.transform(dataFrame)).thenReturn(dataFrame)
    when(streamWriter.write(dataFrame, streamManager)).thenReturn(streamingQuery)
    when(streamingQuery.stop()).thenThrow(classOf[RuntimeException])
    assertThrows[IngestionException](sparkIngestor.ingest(streamReader, streamManager, streamDecoder, streamTransformer, streamWriter))
  }

  it should "invoke the components in correct order" in {
    val sparkIngestor = SparkIngestor(configuration)
    when(streamReader.read(any[SparkSession])).thenReturn(dataStreamReaderMock)
    when(streamManager.configure(eqTo(dataStreamReaderMock), any[Configuration])).thenReturn(dataStreamReaderMock)
    when(streamDecoder.decode(dataStreamReaderMock)).thenReturn(dataFrame)
    when(streamTransformer.transform(dataFrame)).thenReturn(dataFrame)
    when(streamWriter.write(dataFrame, streamManager)).thenReturn(streamingQuery)

    sparkIngestor.ingest(streamReader, streamManager, streamDecoder, streamTransformer, streamWriter)

    val inOrderCheck = Mockito.inOrder(streamReader, streamManager, streamDecoder, streamTransformer, streamWriter)
    inOrderCheck.verify(streamReader).read(any[SparkSession])
    inOrderCheck.verify(streamManager).configure(eqTo(dataStreamReaderMock), any[Configuration])
    inOrderCheck.verify(streamDecoder).decode(dataStreamReaderMock)
    inOrderCheck.verify(streamTransformer).transform(dataFrame)
    inOrderCheck.verify(streamWriter).write(dataFrame, streamManager)
    verify(streamingQuery).processAllAvailable
    verify(streamingQuery).stop
  }

  it should "use the configured app name" in {
    val config = new BaseConfiguration
    config.addProperty(SparkIngestor.KEY_APP_NAME, "my-app-name")
    val sparkIngestor = SparkIngestor(config)

    sparkIngestor.spark.conf.get("spark.app.name") shouldBe "my-app-name"
  }

  it should "throw if no app name is configured" in {
    val throwable = intercept[IllegalArgumentException](SparkIngestor(new BaseConfiguration))

    throwable.getMessage should include(SparkIngestor.KEY_APP_NAME)
  }

  it should "use terminationMethod awaitTermination if configured" in {
    val config = new BaseConfiguration
    config.addProperty(SparkIngestor.KEY_APP_NAME, "my-spark-app")
    config.addProperty(s"${SparkIngestor.KEY_TERMINATION_METHOD}", AwaitTermination)
    val sparkIngestor = SparkIngestor(config)
    when(streamReader.read(any[SparkSession])).thenReturn(dataStreamReaderMock)
    when(streamManager.configure(eqTo(dataStreamReaderMock), any[Configuration])).thenReturn(dataStreamReaderMock)
    when(streamDecoder.decode(dataStreamReaderMock)).thenReturn(dataFrame)
    when(streamTransformer.transform(dataFrame)).thenReturn(dataFrame)
    when(streamWriter.write(dataFrame, streamManager)).thenReturn(streamingQuery)

    sparkIngestor.ingest(streamReader, streamManager, streamDecoder, streamTransformer, streamWriter)

    verify(streamingQuery).awaitTermination()
  }

  it should "use timeout if configured with terminationMethod awaitTermination" in {
    val config = new BaseConfiguration
    config.addProperty(SparkIngestor.KEY_APP_NAME, "my-spark-app")
    config.addProperty(s"${SparkIngestor.KEY_TERMINATION_METHOD}", AwaitTermination)
    config.addProperty(s"${SparkIngestor.KEY_AWAIT_TERMINATION_TIMEOUT}", "10000")
    val sparkIngestor = SparkIngestor(config)
    when(streamReader.read(any[SparkSession])).thenReturn(dataStreamReaderMock)
    when(streamManager.configure(eqTo(dataStreamReaderMock), any[Configuration])).thenReturn(dataStreamReaderMock)
    when(streamDecoder.decode(dataStreamReaderMock)).thenReturn(dataFrame)
    when(streamTransformer.transform(dataFrame)).thenReturn(dataFrame)
    when(streamWriter.write(dataFrame, streamManager)).thenReturn(streamingQuery)

    sparkIngestor.ingest(streamReader, streamManager, streamDecoder, streamTransformer, streamWriter)

    verify(streamingQuery).awaitTermination(eqTo(10000L))
  }

  it should "throw if an invalid terminationMethod is configured" in {
    val config = new BaseConfiguration
    config.addProperty(SparkIngestor.KEY_APP_NAME, "my-spark-app")
    config.addProperty(s"${SparkIngestor.KEY_TERMINATION_METHOD}", "non-existent")
    val throwable = intercept[IllegalArgumentException](SparkIngestor(config))

    throwable.getMessage should include(SparkIngestor.KEY_TERMINATION_METHOD)
  }

  it should "throw if a timeout is not a number" in {
    val config = new BaseConfiguration
    config.addProperty(SparkIngestor.KEY_APP_NAME, "my-spark-app")
    config.addProperty(s"${SparkIngestor.KEY_AWAIT_TERMINATION_TIMEOUT}", "nan")
    val throwable = intercept[IllegalArgumentException](SparkIngestor(config))

    throwable.getMessage should include(SparkIngestor.KEY_AWAIT_TERMINATION_TIMEOUT)
  }

}
