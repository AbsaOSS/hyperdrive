/*
 *  Copyright 2019 ABSA Group Limited
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package za.co.absa.hyperdrive.ingestor.implementation.reader.kafka

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.DataStreamReader
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.FlatSpec
import org.scalatest.mockito.MockitoSugar
import za.co.absa.hyperdrive.ingestor.implementation.reader.kafka.KafkaStreamReaderProps._

class TestKafkaStreamReader extends FlatSpec with MockitoSugar {

  private val validTopic = "test-topic"
  private val validBrokers = "PLAINTEXT://localhost:9092"
  private val validExtraConfs = Map("ssl.truststore.location" -> "whatever/path/you/take",
    "ssl.truststore.password" -> "ourlittlesecret",
    "ssl.keystore.location"   -> "just/another/path",
    "ssl.keystore.password"   -> "knock-knock",
    "ssl.key.password"        -> "you-shall-not-pass")

  behavior of "KafkaStreamReader"

  it should "throw on blank topic" in {
    assertThrows[IllegalArgumentException]( // null topic
      new KafkaStreamReader(topic = null, validBrokers, validExtraConfs)
    )
    assertThrows[IllegalArgumentException]( // empty topic
      new KafkaStreamReader(topic = "  ", validBrokers, validExtraConfs)
    )
  }

  it should "throw on blank brokers" in {
    assertThrows[IllegalArgumentException]( // null topic
      new KafkaStreamReader(validTopic, brokers = null, validExtraConfs)
    )
    assertThrows[IllegalArgumentException]( // empty topic
      new KafkaStreamReader(validTopic, brokers = "  ", validExtraConfs)
    )
  }

  it should "throw on null parameters map" in {
    assertThrows[IllegalArgumentException]( // null topic
      new KafkaStreamReader(validTopic, validBrokers, extraConfs = null)
    )
  }

  it should "throw on null SparkSession" in {
    val reader = new KafkaStreamReader(validTopic, validBrokers, validExtraConfs)
    assertThrows[IllegalArgumentException](reader.read(spark = null))
  }

  it should "throw if SparkSession is stopped" in {
    val reader = new KafkaStreamReader(validTopic, validBrokers, validExtraConfs)
    val sparkContext = getMockedSparkContext(stopped = true)
    val dataStreamReader = getMockedDataStreamReader
    val sparkSession = getConfiguredMockedSparkSession(sparkContext, dataStreamReader)
    assertThrows[IllegalStateException](reader.read(sparkSession))
  }

  it should "set topic, brokers and options on SparkSession" in {
    val sparkContext = getMockedSparkContext(stopped = false)
    val dataStreamReader = getMockedDataStreamReader
    val sparkSession = getConfiguredMockedSparkSession(sparkContext, dataStreamReader)

    val reader = new KafkaStreamReader(validTopic, validBrokers, validExtraConfs)
    reader.read(sparkSession)

    verify(sparkSession).readStream
    verify(dataStreamReader).format(KafkaStreamReaderProps.STREAM_FORMAT_KAFKA_NAME)
    verify(dataStreamReader).option(TOPIC_SUBSCRIPTION_KEY, validTopic)
    verify(dataStreamReader).option(SPARK_BROKERS_SETTING_KEY, validBrokers)

    validExtraConfs.foreach(pair => verify(dataStreamReader).option(pair._1, pair._2))
  }

  it should "set topic and brokers on SparkSession if no extra options informed" in {
    val sparkContext = getMockedSparkContext(stopped = false)
    val dataStreamReader = getMockedDataStreamReader
    val sparkSession = getConfiguredMockedSparkSession(sparkContext, dataStreamReader)

    val reader = new KafkaStreamReader(validTopic, validBrokers, Map[String,String]())
    reader.read(sparkSession)

    verify(sparkSession).readStream
    verify(dataStreamReader).format(STREAM_FORMAT_KAFKA_NAME)
    verify(dataStreamReader).option(TOPIC_SUBSCRIPTION_KEY, validTopic)
    verify(dataStreamReader).option(SPARK_BROKERS_SETTING_KEY, validBrokers)

    validExtraConfs.foreach(conf => verify(dataStreamReader, never()).option(conf._1, conf._2)) // verify never
  }

  it should "include the topic in the source name" in {
    val reader = new KafkaStreamReader(validTopic, validBrokers, validExtraConfs)
    assert(reader.getSourceName.toLowerCase.contains(validTopic))
  }

  private def getMockedSparkContext(stopped: Boolean): SparkContext = {
    val sparkContext = mock[SparkContext]
    when(sparkContext.isStopped).thenReturn(stopped)
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
