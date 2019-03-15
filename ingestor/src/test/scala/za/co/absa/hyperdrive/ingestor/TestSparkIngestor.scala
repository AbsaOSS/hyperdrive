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

package za.co.absa.hyperdrive.ingestor

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.{DataStreamReader, StreamingQuery}
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import za.co.absa.hyperdrive.manager.offset.OffsetManager
import za.co.absa.hyperdrive.reader.StreamReader
import za.co.absa.hyperdrive.transformer.data.StreamTransformer
import za.co.absa.hyperdrive.transformer.encoding.AvroDecoder
import za.co.absa.hyperdrive.writer.StreamWriter
import org.scalatest.mockito.MockitoSugar
import org.mockito.Mockito._

class TestSparkIngestor extends FlatSpec with BeforeAndAfterAll with MockitoSugar {

  private var sparkSession: SparkSession =  _
  private var streamReader: StreamReader = _
  private var offsetManager: OffsetManager = _
  private var avroDecoder: AvroDecoder = _
  private var streamTransformer: StreamTransformer = _
  private var streamWriter: StreamWriter = _

  private val nullMockedDataStream: DataStreamReader = null
  private var dataFrame: DataFrame = _
  private var streamingQuery: StreamingQuery = _

  override def beforeAll: Unit = {
    sparkSession      = mock[SparkSession]
    streamReader      = mock[StreamReader]
    offsetManager     = mock[OffsetManager]
    avroDecoder       = mock[AvroDecoder]
    streamTransformer = mock[StreamTransformer]
    streamWriter      = mock[StreamWriter]
    dataFrame         = mock[DataFrame]
    streamingQuery    = mock[StreamingQuery]
  }

  behavior of SparkIngestor.getClass.getName

  it should "throw on null Spark session" in {
    val throwable = intercept[IllegalArgumentException](
      SparkIngestor.ingest(null)(streamReader)(offsetManager)(avroDecoder)(streamTransformer)(streamWriter)
    )
    assert(throwable.getMessage.toLowerCase.contains("null"))
    assert(throwable.getMessage.toLowerCase.contains("spark"))
    assert(throwable.getMessage.toLowerCase.contains("session"))
  }

  it should "throw on null StreamReader" in {
    val throwable = intercept[IllegalArgumentException](
      SparkIngestor.ingest(sparkSession)(null)(offsetManager)(avroDecoder)(streamTransformer)(streamWriter)
    )
    assert(throwable.getMessage.toLowerCase.contains("null"))
    assert(throwable.getMessage.toLowerCase.contains("stream"))
    assert(throwable.getMessage.toLowerCase.contains("reader"))
  }

  it should "throw on null OffsetManager" in {
    val throwable = intercept[IllegalArgumentException](
      SparkIngestor.ingest(sparkSession)(streamReader)(null)(avroDecoder)(streamTransformer)(streamWriter)
    )
    assert(throwable.getMessage.toLowerCase.contains("null"))
    assert(throwable.getMessage.toLowerCase.contains("offset"))
    assert(throwable.getMessage.toLowerCase.contains("manager"))
  }

  it should "throw on null AvroDecoder" in {
    val throwable = intercept[IllegalArgumentException](
      SparkIngestor.ingest(sparkSession)(streamReader)(offsetManager)(null)(streamTransformer)(streamWriter)
    )
    assert(throwable.getMessage.toLowerCase.contains("null"))
    assert(throwable.getMessage.toLowerCase.contains("avro"))
    assert(throwable.getMessage.toLowerCase.contains("decoder"))
  }

  it should "throw on null StreamTransformer" in {
    val throwable = intercept[IllegalArgumentException](
      SparkIngestor.ingest(sparkSession)(streamReader)(offsetManager)(avroDecoder)(null)(streamWriter)
    )
    assert(throwable.getMessage.toLowerCase.contains("null"))
    assert(throwable.getMessage.toLowerCase.contains("stream"))
    assert(throwable.getMessage.toLowerCase.contains("transformer"))
  }

  it should "throw on null StreamWriter" in {
    val throwable = intercept[IllegalArgumentException](
      SparkIngestor.ingest(sparkSession)(streamReader)(offsetManager)(avroDecoder)(streamTransformer)(null)
    )
    assert(throwable.getMessage.toLowerCase.contains("null"))
    assert(throwable.getMessage.toLowerCase.contains("stream"))
    assert(throwable.getMessage.toLowerCase.contains("writer"))
  }

  it should "invoke the components in correct order" in {
    prepareMocks()

    // order in which the components should be invoked
    val inOrderCheck = inOrder(streamReader, sparkSession, offsetManager, avroDecoder, streamTransformer, streamWriter)

    SparkIngestor.ingest(sparkSession)(streamReader)(offsetManager)(avroDecoder)(streamTransformer)(streamWriter)

    val nullMockedDataStream: DataStreamReader = null

    inOrderCheck.verify(streamReader).read(sparkSession)
    inOrderCheck.verify(offsetManager).configureOffsets(nullMockedDataStream)
    inOrderCheck.verify(avroDecoder).decode(nullMockedDataStream)
    inOrderCheck.verify(streamTransformer).transform(dataFrame)
    inOrderCheck.verify(streamWriter).write(dataFrame, offsetManager)

    verify(streamingQuery).processAllAvailable
    verify(streamingQuery).stop
  }

  private def prepareMocks(): Unit = {
    when(streamReader.read(sparkSession)).thenReturn(nullMockedDataStream)
    when(streamReader.getSourceName).thenReturn("mocked_topic")

    when(offsetManager.configureOffsets(nullMockedDataStream)).thenReturn(nullMockedDataStream)
    when(avroDecoder.decode(nullMockedDataStream)).thenReturn(dataFrame)
    when(streamTransformer.transform(dataFrame)).thenReturn(dataFrame)

    when(streamWriter.write(dataFrame, offsetManager)).thenReturn(streamingQuery)
    when(streamWriter.getDestination).thenReturn("mocked_destination")
  }
}
