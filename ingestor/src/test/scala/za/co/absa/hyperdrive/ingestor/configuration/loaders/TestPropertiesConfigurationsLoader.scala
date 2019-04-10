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

package za.co.absa.hyperdrive.ingestor.configuration.loaders

import java.io.{File, FileOutputStream}
import java.util.{Properties, UUID}

import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec}
import za.co.absa.abris.avro.schemas.policy.SchemaRetentionPolicies
import za.co.absa.hyperdrive.ingestor.configuration.components._
import za.co.absa.hyperdrive.shared.utils.TempDir

class TestPropertiesConfigurationsLoader extends FlatSpec with BeforeAndAfterAll with BeforeAndAfterEach {

  private val tempDir = TempDir.getNew
  private var propertiesFile: File = _

  private var expectedSparkConf: SparkConf = _
  private var expectedStreamReaderConf: KafkaStreamReaderConf = _
  private var expectedOffsetManagerConf: CheckpointingOffsetManagerConf = _
  private var expectedStreamDecoderConf: AvroStreamDecoderConf = _
  private var expectedStreamWriterConf: ParquetStreamWriterConf = _
  private var expecteStreamTransformerConf: ColumnSelectorStreamTransformerConf = _

  private var payloadTestTopic: String = _

  override def afterAll(): Unit = FileUtils.deleteDirectory(tempDir)

  override def beforeEach(): Unit = {
    propertiesFile = getPropertiesFile
    initializeConfs()
    generateCompletePropertiesFile(Set[String]())
  }
  override def afterEach(): Unit = FileUtils.cleanDirectory(tempDir)

  private def initializeConfs(): Unit = {
    payloadTestTopic = randomString
    expectedSparkConf = getSparkSessionConf
    expectedStreamReaderConf = getStreamReaderConf
    expectedOffsetManagerConf = getOffsetManagerConf
    expectedStreamDecoderConf = getStreamDecoderConf
    expectedStreamWriterConf = getStreamWriterConf
    expecteStreamTransformerConf = getStreamTransformerConf
  }

  behavior of PropertiesConfigurationsLoader.getClass.getSimpleName

  it should "load Spark configurations" in {
    val actualConf = PropertiesConfigurationsLoader.load(propertiesFile.getAbsolutePath).sparkConf

    assert(expectedSparkConf.appName == actualConf.appName)
  }

  it should "load stream reader configurations" in {
    val actualConf = PropertiesConfigurationsLoader.load(propertiesFile.getAbsolutePath).streamReaderConf

    assert(expectedStreamReaderConf.topic == actualConf.topic)
    assert(expectedStreamReaderConf.brokers == actualConf.brokers)
    assert(expectedStreamReaderConf.extraConfs.size == actualConf.extraConfs.size)
    expectedStreamReaderConf.extraConfs.foreach {case (key,value) => assert(value == actualConf.extraConfs(key))}
  }

  it should "load offset manager configurations" in {
    val actualConf = PropertiesConfigurationsLoader.load(propertiesFile.getAbsolutePath).offsetManagerConf

    assert(expectedOffsetManagerConf.topic == actualConf.topic)
    assert(expectedOffsetManagerConf.checkpointBaseLocation == actualConf.checkpointBaseLocation)
  }

  it should "load stream decoder configurations" in {
    val actualConf = PropertiesConfigurationsLoader.load(propertiesFile.getAbsolutePath).streamDecoderConf

    assert(expectedStreamDecoderConf.retentionPolicy == actualConf.retentionPolicy)
    assert(equals(expectedStreamDecoderConf.schemaRegistrySettings, actualConf.schemaRegistrySettings))
  }

  it should "load stream writer configurations" in {
    val actualConf = PropertiesConfigurationsLoader.load(propertiesFile.getAbsolutePath).streamWriterConf

    assert(expectedStreamWriterConf.destination == actualConf.destination)
    assert(expectedStreamWriterConf.extraConfOptions.isDefined == actualConf.extraConfOptions.isDefined)
    assert(equals(expectedStreamWriterConf.extraConfOptions.get, actualConf.extraConfOptions.get))
  }

  it should "load stream transformer configurations" in {
    val actualConf = PropertiesConfigurationsLoader.load(propertiesFile.getAbsolutePath).streamTransformerConf

    assert(expecteStreamTransformerConf.columns == actualConf.columns)
  }

  it should "not throw on no extra configurations for stream reader" in {
    beforeEach() // deletes the current properties file
    expectedStreamReaderConf = getStreamReaderConf // creates the configuration for the stream reader
    generateCompletePropertiesFile(Set[String](StreamReaderKeys.KEY_EXTRA_CONFS)) // re-writes the properties file

    val actualConf = PropertiesConfigurationsLoader.load(propertiesFile.getAbsolutePath).streamReaderConf
    assert(expectedStreamReaderConf.topic == actualConf.topic)
    assert(expectedStreamReaderConf.brokers == actualConf.brokers)
    assert(actualConf.extraConfs.isEmpty)
  }

  it should "not throw on non-existent optional maps for stream writer" in {
    beforeEach()
    expectedStreamWriterConf = getStreamWriterWithoutExtraConf
    generateCompletePropertiesFile(Set())

    val actualConf = PropertiesConfigurationsLoader.load(propertiesFile.getAbsolutePath).streamWriterConf

    assert(expectedStreamWriterConf.destination == actualConf.destination)
    assert(expectedStreamWriterConf.extraConfOptions.isDefined == actualConf.extraConfOptions.isDefined)
    assert(actualConf.extraConfOptions.isEmpty)
  }

  private def generateCompletePropertiesFile(propertiesToSkip: Set[String]): Unit = {
    val allConfs = Seq(convertToSparkConfProperties(expectedSparkConf),
      convertToStreamReaderProperties(expectedStreamReaderConf),
      convertToOffsetManagerProperties(expectedOffsetManagerConf),
      convertToStreamDecoderProperties(expectedStreamDecoderConf),
      convertToStreamWriterProperties(expectedStreamWriterConf),
      convertToStreamTransformerProperties(expecteStreamTransformerConf))

    val properties = new Properties()

    import scala.collection.JavaConverters._
    allConfs.foreach(conf => {
      val retained = conf.filterKeys(!propertiesToSkip.contains(_))
      properties.putAll(retained.asJava)
    })

    writeProperties(properties)
  }

  private def writeProperties(properties: Properties): Unit = {
    val outStream = new FileOutputStream(propertiesFile)
    properties.store(outStream, "test properties")
    outStream.close()
  }

  private def getPropertiesFile: File = new File(tempDir, "Ingestion.properties.template")

  private def getSparkSessionConf: SparkConf = {
    SparkConf(appName = "test_app-name")
  }

  private def getStreamReaderConf: KafkaStreamReaderConf = {
    KafkaStreamReaderConf(topic = payloadTestTopic,
      brokers = "PLAINTEXT://broker1:9091, SSL://broker2.9092",
      Map("key1" -> "value1", "key2" -> "value2"))
  }

  private def getOffsetManagerConf: CheckpointingOffsetManagerConf = {
    CheckpointingOffsetManagerConf(topic = payloadTestTopic, checkpointBaseLocation = "/tmp/test-checkpoint")
  }

  private def getStreamDecoderConf: AvroStreamDecoderConf = {
    AvroStreamDecoderConf(randomString, Map(randomString -> randomString, randomString -> randomString),
      SchemaRetentionPolicies.RETAIN_SELECTED_COLUMN_ONLY)
  }

  private def getStreamWriterConf: ParquetStreamWriterConf = {
    ParquetStreamWriterConf(randomString, Some(Map[String,String](randomString -> randomString)))
  }

  private def getStreamWriterWithoutExtraConf: ParquetStreamWriterConf = {
    ParquetStreamWriterConf(randomString, None)
  }

  private def getStreamTransformerConf: ColumnSelectorStreamTransformerConf = {
    ColumnSelectorStreamTransformerConf(columns = Seq("a","b","c"))
  }

  private def convertToSparkConfProperties(sparkConf: SparkConf): Map[String,String] = {
    import SparkConfKeys._
    import sparkConf._
    Map(KEY_SPARK_APP_NAME -> appName)
  }

  private def convertToStreamReaderProperties(readerConf: KafkaStreamReaderConf): Map[String,String] = {
    import StreamReaderKeys._
    import readerConf._
    Map(KEY_SOURCE_TOPIC -> topic,
      KEY_SOURCE_BROKERS -> brokers,
      KEY_EXTRA_CONFS -> mapToKeyValueList(readerConf.extraConfs))
  }

  private def convertToOffsetManagerProperties(offsetManagerConf: CheckpointingOffsetManagerConf): Map[String,String] = {
    import OffsetManagerKeys._
    import offsetManagerConf._
    Map(KEY_OFFSET_MANAGER_TOPIC -> topic,
      KEY_CHECKPOINT_BASE_LOCATION -> checkpointBaseLocation)
  }

  private def convertToStreamDecoderProperties(streamDecoderConf: AvroStreamDecoderConf): Map[String,String] = {
    import StreamDecoderKeys._
    import streamDecoderConf._
    Map[String,String](KEY_SCHEMA_RETENTION_POLICY -> retentionPolicy.toString,
      KEY_SCHEMA_REGISTRY_SETTINGS -> mapToKeyValueList(schemaRegistrySettings))
  }

  private def convertToStreamWriterProperties(streamWriterConf: ParquetStreamWriterConf): Map[String,String] = {
    import StreamWriterKeys._
    import streamWriterConf._
    val builder: Map[String,String] = extraConfOptions match {
      case Some(confs: Map[String,String]) => Map[String,String](KEY_EXTRA_CONFS -> mapToKeyValueList(confs))
      case None => Map[String,String]()
    }
    builder + (KEY_DESTINATION_DIRECTORY -> destination)
  }

  private def convertToStreamTransformerProperties(streamTransformerConf: ColumnSelectorStreamTransformerConf): Map[String,String] = {
    import StreamTransformerKeys._
    import streamTransformerConf._
    Map(KEY_TRANSFORMATION_QUERY -> streamTransformerConf.columns.mkString(","))
  }

  private def mapToKeyValueList(map: Map[String,String]): String = {
    map.foldLeft("") {
      case (acc, (key,value)) => acc + " " + key + "=" + value
    }
  }

  private def equals(map1: Map[String,String], map2: Map[String,String]): Boolean = map1.toSet.diff(map2.toSet).isEmpty

  private def randomString: String = UUID.randomUUID().toString
}
