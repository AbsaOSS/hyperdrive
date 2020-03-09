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

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.{DataStreamReader, StreamingQuery}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FlatSpec}
import za.co.absa.hyperdrive.ingestor.api.decoder.StreamDecoder
import za.co.absa.hyperdrive.ingestor.api.manager.StreamManager
import za.co.absa.hyperdrive.ingestor.api.reader.StreamReader
import za.co.absa.hyperdrive.ingestor.api.transformer.StreamTransformer
import za.co.absa.hyperdrive.ingestor.api.writer.StreamWriter
import za.co.absa.hyperdrive.shared.exceptions.{IngestionException, IngestionStartException}

class TestSparkIngestor extends FlatSpec with BeforeAndAfterEach with MockitoSugar {

  private val sparkSession: SparkSession = mock[SparkSession]
  private val streamReader: StreamReader = mock[StreamReader]
  private val streamManager: StreamManager = mock[StreamManager]
  private val streamDecoder: StreamDecoder = mock[StreamDecoder]
  private val streamTransformer: StreamTransformer = mock[StreamTransformer]
  private val streamWriter: StreamWriter = mock[StreamWriter]

  private val dataStreamReaderMock: DataStreamReader = mock[DataStreamReader]
  private val dataFrame: DataFrame = mock[DataFrame]
  private val streamingQuery: StreamingQuery = mock[StreamingQuery]

  private val configuration = new Configuration()

  override def beforeEach(): Unit = {
    reset(sparkSession,
      streamReader,
      streamManager,
      streamDecoder,
      streamTransformer,
      streamWriter,
      dataFrame,
      streamingQuery)
    val sparkContext = mock[SparkContext]
    when(sparkContext.hadoopConfiguration).thenReturn(configuration)
    when(sparkSession.sparkContext).thenReturn(sparkContext)
  }

  behavior of SparkIngestor.getClass.getName

  it should "throw IngestionStartException if stream reader fails during setup" in {
    when(streamReader.read(sparkSession)).thenThrow(classOf[RuntimeException])
    assertThrows[IngestionStartException](SparkIngestor.ingest(sparkSession, streamReader, streamManager, streamDecoder, streamTransformer, streamWriter))
  }

  it should "throw IngestionStartException if stream manager fails during setup" in {
    when(streamReader.read(sparkSession)).thenReturn(dataStreamReaderMock)
    when(streamManager.configure(dataStreamReaderMock, configuration)).thenThrow(classOf[RuntimeException])
    assertThrows[IngestionStartException](SparkIngestor.ingest(sparkSession, streamReader, streamManager, streamDecoder, streamTransformer, streamWriter))
  }

  it should "throw IngestionStartException if format decoder fails during setup" in {
    when(streamReader.read(sparkSession)).thenReturn(dataStreamReaderMock)
    when(streamManager.configure(dataStreamReaderMock, configuration)).thenReturn(dataStreamReaderMock)
    when(streamDecoder.decode(dataStreamReaderMock)).thenThrow(classOf[RuntimeException])
    assertThrows[IngestionStartException](SparkIngestor.ingest(sparkSession, streamReader, streamManager, streamDecoder, streamTransformer, streamWriter))
  }

  it should "throw IngestionStartException if stream transformer fails during setup" in {
    when(streamReader.read(sparkSession)).thenReturn(dataStreamReaderMock)
    when(streamManager.configure(dataStreamReaderMock, configuration)).thenReturn(dataStreamReaderMock)
    when(streamDecoder.decode(dataStreamReaderMock)).thenReturn(dataFrame)
    when(streamTransformer.transform(dataFrame)).thenThrow(classOf[RuntimeException])
    assertThrows[IngestionStartException](SparkIngestor.ingest(sparkSession, streamReader, streamManager, streamDecoder, streamTransformer, streamWriter))
  }

  it should "throw IngestionStartException if stream writer fails during setup" in {
    when(streamReader.read(sparkSession)).thenReturn(dataStreamReaderMock)
    when(streamManager.configure(dataStreamReaderMock, configuration)).thenReturn(dataStreamReaderMock)
    when(streamDecoder.decode(dataStreamReaderMock)).thenReturn(dataFrame)
    when(streamTransformer.transform(dataFrame)).thenReturn(dataFrame)
    when(streamWriter.write(dataFrame, streamManager)).thenThrow(classOf[RuntimeException])
    assertThrows[IngestionStartException](SparkIngestor.ingest(sparkSession, streamReader, streamManager, streamDecoder, streamTransformer, streamWriter))
  }

  it should "throw IngestionException if ingestion fails during execution" in {
    when(streamReader.read(sparkSession)).thenReturn(dataStreamReaderMock)
    when(streamManager.configure(dataStreamReaderMock, configuration)).thenReturn(dataStreamReaderMock)
    when(streamDecoder.decode(dataStreamReaderMock)).thenReturn(dataFrame)
    when(streamTransformer.transform(dataFrame)).thenReturn(dataFrame)
    when(streamWriter.write(dataFrame, streamManager)).thenReturn(streamingQuery)
    when(streamingQuery.stop()).thenThrow(classOf[RuntimeException])
    assertThrows[IngestionException](SparkIngestor.ingest(sparkSession, streamReader, streamManager, streamDecoder, streamTransformer, streamWriter))
  }

  it should "invoke the components in correct order" in {
    when(streamReader.read(sparkSession)).thenReturn(dataStreamReaderMock)
    when(streamManager.configure(dataStreamReaderMock, configuration)).thenReturn(dataStreamReaderMock)
    when(streamDecoder.decode(dataStreamReaderMock)).thenReturn(dataFrame)
    when(streamTransformer.transform(dataFrame)).thenReturn(dataFrame)
    when(streamWriter.write(dataFrame, streamManager)).thenReturn(streamingQuery)

    SparkIngestor.ingest(sparkSession, streamReader, streamManager, streamDecoder, streamTransformer, streamWriter)

    val inOrderCheck = inOrder(streamReader, sparkSession, streamManager, streamDecoder, streamTransformer, streamWriter)
    inOrderCheck.verify(streamReader).read(sparkSession)
    inOrderCheck.verify(streamManager).configure(dataStreamReaderMock, configuration)
    inOrderCheck.verify(streamDecoder).decode(dataStreamReaderMock)
    inOrderCheck.verify(streamTransformer).transform(dataFrame)
    inOrderCheck.verify(streamWriter).write(dataFrame, streamManager)
    verify(streamingQuery).processAllAvailable
    verify(streamingQuery).stop
  }
}
