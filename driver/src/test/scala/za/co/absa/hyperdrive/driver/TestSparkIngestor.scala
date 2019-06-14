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

package za.co.absa.hyperdrive.driver

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.{DataStreamReader, StreamingQuery}
import org.scalatest.{BeforeAndAfterEach, FlatSpec}
import org.scalatest.mockito.MockitoSugar
import org.mockito.Mockito._
import za.co.absa.hyperdrive.ingestor.api.decoder.StreamDecoder
import za.co.absa.hyperdrive.ingestor.api.manager.OffsetManager
import za.co.absa.hyperdrive.ingestor.api.reader.StreamReader
import za.co.absa.hyperdrive.ingestor.api.transformer.StreamTransformer
import za.co.absa.hyperdrive.ingestor.api.writer.StreamWriter
import za.co.absa.hyperdrive.shared.exceptions.{IngestionException, IngestionStartException}

class TestSparkIngestor extends FlatSpec with BeforeAndAfterEach with MockitoSugar {

  private val sparkSession: SparkSession = mock[SparkSession]
  private val streamReader: StreamReader = mock[StreamReader]
  private val offsetManager: OffsetManager = mock[OffsetManager]
  private val streamDecoder: StreamDecoder = mock[StreamDecoder]
  private val streamTransformer: StreamTransformer = mock[StreamTransformer]
  private val streamWriter: StreamWriter = mock[StreamWriter]

  private val nullMockedDataStream: DataStreamReader = null
  private val dataFrame: DataFrame = mock[DataFrame]
  private val streamingQuery: StreamingQuery = mock[StreamingQuery]

  private val configuration = new Configuration()

  override def beforeEach(): Unit = {
    reset(sparkSession,
      streamReader,
      offsetManager,
      streamDecoder,
      streamTransformer,
      streamWriter,
      dataFrame,
      streamingQuery)
    when(streamReader.getSourceName).thenReturn("test-source")
    when(streamWriter.getDestination).thenReturn("test-destination")
  }

  behavior of SparkIngestor.getClass.getName

  it should "throw on null Spark session" in {
    val throwable = intercept[IllegalArgumentException](
      SparkIngestor.ingest(spark = null, streamReader, offsetManager, streamDecoder, streamTransformer, streamWriter)
    )
    assert(throwable.getMessage.toLowerCase.contains("null"))
    assert(throwable.getMessage.toLowerCase.contains("spark"))
    assert(throwable.getMessage.toLowerCase.contains("session"))
  }

  it should "throw on null StreamReader" in {
    val throwable = intercept[IllegalArgumentException](
      SparkIngestor.ingest(sparkSession, streamReader = null, offsetManager, streamDecoder, streamTransformer, streamWriter)
    )
    assert(throwable.getMessage.toLowerCase.contains("null"))
    assert(throwable.getMessage.toLowerCase.contains("stream"))
    assert(throwable.getMessage.toLowerCase.contains("reader"))
  }

  it should "throw on null OffsetManager" in {
    val throwable = intercept[IllegalArgumentException](
      SparkIngestor.ingest(sparkSession, streamReader, offsetManager = null, streamDecoder, streamTransformer, streamWriter)
    )
    assert(throwable.getMessage.toLowerCase.contains("null"))
    assert(throwable.getMessage.toLowerCase.contains("offset"))
    assert(throwable.getMessage.toLowerCase.contains("manager"))
  }

  it should "throw on null AvroDecoder" in {
    val throwable = intercept[IllegalArgumentException](
      SparkIngestor.ingest(sparkSession, streamReader, offsetManager, decoder = null, streamTransformer, streamWriter)
    )
    assert(throwable.getMessage.toLowerCase.contains("null"))
    assert(throwable.getMessage.toLowerCase.contains("decoder"))
  }

  it should "throw on null StreamTransformer" in {
    val throwable = intercept[IllegalArgumentException](
      SparkIngestor.ingest(sparkSession, streamReader, offsetManager, streamDecoder, streamTransformer = null, streamWriter)
    )
    assert(throwable.getMessage.toLowerCase.contains("null"))
    assert(throwable.getMessage.toLowerCase.contains("stream"))
    assert(throwable.getMessage.toLowerCase.contains("transformer"))
  }

  it should "throw on null StreamWriter" in {
    val throwable = intercept[IllegalArgumentException](
      SparkIngestor.ingest(sparkSession, streamReader, offsetManager, streamDecoder, streamTransformer, streamWriter = null)
    )
    assert(throwable.getMessage.toLowerCase.contains("null"))
    assert(throwable.getMessage.toLowerCase.contains("stream"))
    assert(throwable.getMessage.toLowerCase.contains("writer"))
  }

  it should "throw IngestionStartException if stream reader fails during setup" in {
    when(streamReader.read(sparkSession)).thenReturn(nullMockedDataStream)
    when(streamReader.read(sparkSession)).thenThrow(classOf[NullPointerException])
    assertThrows[IngestionStartException](SparkIngestor.ingest(sparkSession, streamReader, offsetManager, streamDecoder, streamTransformer, streamWriter))
  }

  it should "throw IngestionStartException if offset manager fails during setup" in {
    when(offsetManager.configureOffsets(nullMockedDataStream, configuration)).thenReturn(nullMockedDataStream)
    when(offsetManager.configureOffsets(nullMockedDataStream, configuration)).thenThrow(classOf[NullPointerException])
    assertThrows[IngestionStartException](SparkIngestor.ingest(sparkSession, streamReader, offsetManager, streamDecoder, streamTransformer, streamWriter))
  }

  it should "throw IngestionStartException if format decoder fails during setup" in {
    when(streamDecoder.decode(nullMockedDataStream)).thenReturn(dataFrame)
    when(streamDecoder.decode(nullMockedDataStream)).thenThrow(classOf[NullPointerException])
    assertThrows[IngestionStartException](SparkIngestor.ingest(sparkSession, streamReader, offsetManager, streamDecoder, streamTransformer, streamWriter))
  }

  it should "throw IngestionStartException if stream transformer fails during setup" in {
    when(streamDecoder.decode(nullMockedDataStream)).thenReturn(dataFrame)
    when(streamTransformer.transform(dataFrame)).thenReturn(dataFrame)
    when(streamTransformer.transform(dataFrame)).thenThrow(classOf[NullPointerException])
    assertThrows[IngestionStartException](SparkIngestor.ingest(sparkSession, streamReader, offsetManager, streamDecoder, streamTransformer, streamWriter))
  }

  it should "throw IngestionStartException if stream writer fails during setup" in {
    when(streamDecoder.decode(nullMockedDataStream)).thenReturn(dataFrame)
    when(streamTransformer.transform(dataFrame)).thenReturn(dataFrame)
    when(streamWriter.write(dataFrame, offsetManager)).thenReturn(streamingQuery)
    when(streamWriter.write(dataFrame, offsetManager)).thenThrow(classOf[NullPointerException])
    assertThrows[IngestionStartException](SparkIngestor.ingest(sparkSession, streamReader, offsetManager, streamDecoder, streamTransformer, streamWriter))
  }

  it should "throw IngestionException if ingestion fails during execution" in {
    when(streamDecoder.decode(nullMockedDataStream)).thenReturn(dataFrame)
    when(streamTransformer.transform(dataFrame)).thenReturn(dataFrame)
    when(streamWriter.write(dataFrame, offsetManager)).thenReturn(streamingQuery)
    when(streamingQuery.stop()).thenThrow(classOf[IllegalStateException])
    assertThrows[IngestionException](SparkIngestor.ingest(sparkSession, streamReader, offsetManager, streamDecoder, streamTransformer, streamWriter))
  }

  it should "invoke the components in correct order" in {
    prepareMocks()

    // order in which the components should be invoked
    val inOrderCheck = inOrder(streamReader, sparkSession, offsetManager, streamDecoder, streamTransformer, streamWriter)

    SparkIngestor.ingest(sparkSession, streamReader, offsetManager, streamDecoder, streamTransformer, streamWriter)

    val nullMockedDataStream: DataStreamReader = null

    inOrderCheck.verify(streamReader).read(sparkSession)
    inOrderCheck.verify(offsetManager).configureOffsets(nullMockedDataStream, configuration)
    inOrderCheck.verify(streamDecoder).decode(nullMockedDataStream)
    inOrderCheck.verify(streamTransformer).transform(dataFrame)
    inOrderCheck.verify(streamWriter).write(dataFrame, offsetManager)

    verify(streamingQuery).processAllAvailable
    verify(streamingQuery).stop
  }

  private def prepareMocks(): Unit = {
    val sparkContext = mock[SparkContext]
    when(sparkContext.hadoopConfiguration).thenReturn(configuration)

    when(sparkSession.sparkContext).thenReturn(sparkContext)

    when(streamReader.read(sparkSession)).thenReturn(nullMockedDataStream)
    when(streamReader.getSourceName).thenReturn("mocked_topic")

    when(offsetManager.configureOffsets(nullMockedDataStream, configuration)).thenReturn(nullMockedDataStream)
    when(streamDecoder.decode(nullMockedDataStream)).thenReturn(dataFrame)
    when(streamTransformer.transform(dataFrame)).thenReturn(dataFrame)

    when(streamWriter.write(dataFrame, offsetManager)).thenReturn(streamingQuery)
    when(streamWriter.getDestination).thenReturn("mocked_destination")
  }
}
