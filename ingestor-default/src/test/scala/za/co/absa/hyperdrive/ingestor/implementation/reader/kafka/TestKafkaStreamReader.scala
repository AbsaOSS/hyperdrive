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

package za.co.absa.hyperdrive.ingestor.implementation.reader.kafka

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.DataStreamReader
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FlatSpec}
import za.co.absa.commons.io.TempDirectory
import za.co.absa.hyperdrive.ingestor.implementation.reader.kafka.KafkaStreamReaderProps._

class TestKafkaStreamReader extends FlatSpec with BeforeAndAfterEach with MockitoSugar {

  private val validTopic = "test-topic"
  private val validBrokers = "PLAINTEXT://localhost:9092"
  private val validExtraConfs = Map("ssl.truststore.location" -> "whatever/path/you/take",
    "ssl.truststore.password" -> "ourlittlesecret",
    "ssl.keystore.location"   -> "just/another/path",
    "ssl.keystore.password"   -> "knock-knock",
    "ssl.key.password"        -> "you-shall-not-pass",
    "failOnDataLoss"          -> "false")
  private var tempDir: TempDirectory = _
  private var tempDirPath: String = _

  behavior of "KafkaStreamReader"

  override def beforeEach: Unit = {
    tempDir = TempDirectory()
    tempDirPath = tempDir.path.toAbsolutePath.toString
  }

  override def afterEach: Unit = tempDir.delete()

  it should "throw on blank topic" in {
    assertThrows[IllegalArgumentException]( // empty topic
      new KafkaStreamReader(topic = "  ", validBrokers, tempDirPath, validExtraConfs)
    )
  }

  it should "throw on blank brokers" in {
    assertThrows[IllegalArgumentException]( // empty topic
      new KafkaStreamReader(validTopic, brokers = "  ", tempDirPath, validExtraConfs)
    )
  }

  it should "throw if SparkSession is stopped" in {
    val reader = new KafkaStreamReader(validTopic, validBrokers, tempDirPath, validExtraConfs)
    val sparkContext = getMockedSparkContext(stopped = true)
    val dataStreamReader = getMockedDataStreamReader
    val sparkSession = getConfiguredMockedSparkSession(sparkContext, dataStreamReader)
    assertThrows[IllegalStateException](reader.read(sparkSession))
  }

  it should "set topic, brokers and options on SparkSession" in {
    val sparkContext = getMockedSparkContext(stopped = false)
    val dataStreamReader = getMockedDataStreamReader
    val sparkSession = getConfiguredMockedSparkSession(sparkContext, dataStreamReader)

    val reader = new KafkaStreamReader(validTopic, validBrokers, tempDirPath, validExtraConfs)
    reader.read(sparkSession)

    verify(sparkSession).readStream
    verify(dataStreamReader).format(KafkaStreamReaderProps.STREAM_FORMAT_KAFKA_NAME)
    verify(dataStreamReader).option(TOPIC_SUBSCRIPTION_KEY, validTopic)
    verify(dataStreamReader).option(SPARK_BROKERS_SETTING_KEY, validBrokers)
    verify(dataStreamReader).options(validExtraConfs)
  }

  it should "set topic and brokers on SparkSession if no extra options informed" in {
    val sparkContext = getMockedSparkContext(stopped = false)
    val dataStreamReader = getMockedDataStreamReader
    val sparkSession = getConfiguredMockedSparkSession(sparkContext, dataStreamReader)

    val reader = new KafkaStreamReader(validTopic, validBrokers, tempDirPath, Map[String,String]())
    reader.read(sparkSession)

    verify(sparkSession).readStream
    verify(dataStreamReader).format(STREAM_FORMAT_KAFKA_NAME)
    verify(dataStreamReader).option(TOPIC_SUBSCRIPTION_KEY, validTopic)
    verify(dataStreamReader).option(SPARK_BROKERS_SETTING_KEY, validBrokers)
    verify(dataStreamReader, never()).options(validExtraConfs)
  }

  it should "set offsets to earliest if no checkpoint location exists" in {
    val sparkContext = getMockedSparkContext(stopped = false)
    val dataStreamReader = getMockedDataStreamReader
    val sparkSession = getConfiguredMockedSparkSession(sparkContext, dataStreamReader)

    val nonExistent = tempDir.path.resolve("non-existent")
    val reader = new KafkaStreamReader(validTopic, validBrokers, nonExistent.toUri.getPath, Map())
    reader.read(sparkSession)

    verify(dataStreamReader).option(WORD_STARTING_OFFSETS, STARTING_OFFSETS_EARLIEST)
  }

  it should "set offsets to user-defined property if no checkpoint location exists" in {
    val sparkContext = getMockedSparkContext(stopped = false)
    val dataStreamReader = getMockedDataStreamReader
    val sparkSession = getConfiguredMockedSparkSession(sparkContext, dataStreamReader)

    val nonExistent = tempDir.path.resolve("non-existent")
    val reader = new KafkaStreamReader(validTopic, validBrokers, nonExistent.toUri.getPath, Map(WORD_STARTING_OFFSETS -> "latest"))
    reader.read(sparkSession)

    verify(dataStreamReader).options(Map(WORD_STARTING_OFFSETS -> "latest"))
  }

  it should "not set offsets if a checkpoint location exists" in {
    val sparkContext = getMockedSparkContext(stopped = false)
    val dataStreamReader = getMockedDataStreamReader
    val sparkSession = getConfiguredMockedSparkSession(sparkContext, dataStreamReader)

    val reader = new KafkaStreamReader(validTopic, validBrokers, tempDirPath, Map())
    reader.read(sparkSession)

    verify(dataStreamReader, never()).option(WORD_STARTING_OFFSETS, STARTING_OFFSETS_EARLIEST)
  }

  private def getMockedSparkContext(stopped: Boolean): SparkContext = {
    val sparkContext = mock[SparkContext]
    val hadoopConf = new Configuration()
    when(sparkContext.isStopped).thenReturn(stopped)
    when(sparkContext.hadoopConfiguration).thenReturn(hadoopConf)
    sparkContext
  }

  private def getMockedDataStreamReader: DataStreamReader = {
    val dataStreamReader = mock[DataStreamReader]
    when(dataStreamReader.format(STREAM_FORMAT_KAFKA_NAME)).thenReturn(dataStreamReader)
    when(dataStreamReader.option(anyString(), anyString())).thenReturn(dataStreamReader)
    dataStreamReader
  }

  private def getConfiguredMockedSparkSession(sparkContext: SparkContext, dataStreamReader: DataStreamReader) = {
    val sparkSession = mock[SparkSession]
    when(sparkSession.sparkContext).thenReturn(sparkContext)
    when(sparkSession.readStream).thenReturn(dataStreamReader)
    sparkSession
  }
}
