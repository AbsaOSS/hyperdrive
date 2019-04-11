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

import java.util.Properties

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec}
import za.co.absa.hyperdrive.ingestor.configuration.components._
import za.co.absa.hyperdrive.ingestor.configuration.loaders.ConfigurationsKeys._
import za.co.absa.hyperdrive.ingestor.configuration.loaders.StubConfigurationBeansFactory._

abstract class BaseTestConfigurationsLoader extends FlatSpec with BeforeAndAfterAll with BeforeAndAfterEach  {

  protected var expectedSparkConf: SparkConf = _
  protected var expectedStreamReaderConf: KafkaStreamReaderConf = _
  protected var expectedOffsetManagerConf: CheckpointingOffsetManagerConf = _
  protected var expectedStreamDecoderConf: AvroStreamDecoderConf = _
  protected var expectedStreamWriterConf: ParquetStreamWriterConf = _
  protected var expectedStreamTransformerConf: ColumnSelectorStreamTransformerConf = _

  protected def initializeConfs(): Unit = {
    resetTestTopic()
    expectedSparkConf = getSparkSessionConf
    expectedStreamReaderConf = getStreamReaderConf
    expectedOffsetManagerConf = getOffsetManagerConf
    expectedStreamDecoderConf = getStreamDecoderConf
    expectedStreamWriterConf = getStreamWriterConf
    expectedStreamTransformerConf = getStreamTransformerConf
  }

  protected def generatePropertiesFromConfs(propertiesToSkip: Set[String]): Properties = {
    val allConfs = Seq(convertToSparkConfMap(expectedSparkConf),
      convertToStreamReaderConfMap(expectedStreamReaderConf),
      convertToOffsetManagerConfMap(expectedOffsetManagerConf),
      convertToStreamDecoderConfMap(expectedStreamDecoderConf),
      convertToStreamWriterConfMap(expectedStreamWriterConf),
      convertToStreamTransformerConfMap(expectedStreamTransformerConf))

    val properties = new Properties()

    import scala.collection.JavaConverters._
    allConfs.foreach(conf => {
      val retained = conf.filterKeys(!propertiesToSkip.contains(_))
      properties.putAll(retained.asJava)
    })

    properties
  }

  protected def convertToSparkConfMap(sparkConf: SparkConf): Map[String,String] = {
    import SparkConfKeys._
    import sparkConf._
    Map(KEY_SPARK_APP_NAME -> appName)
  }

  protected def convertToStreamReaderConfMap(readerConf: KafkaStreamReaderConf): Map[String,String] = {
    import StreamReaderKeys._
    import readerConf._
    Map(KEY_SOURCE_TOPIC -> topic,
      KEY_SOURCE_BROKERS -> brokers,
      KEY_EXTRA_CONFS -> mapToKeyValueList(readerConf.extraConfs))
  }

  protected def convertToOffsetManagerConfMap(offsetManagerConf: CheckpointingOffsetManagerConf): Map[String,String] = {
    import OffsetManagerKeys._
    import offsetManagerConf._
    Map(KEY_OFFSET_MANAGER_TOPIC -> topic,
      KEY_CHECKPOINT_BASE_LOCATION -> checkpointBaseLocation)
  }

  protected def convertToStreamDecoderConfMap(streamDecoderConf: AvroStreamDecoderConf): Map[String,String] = {
    import StreamDecoderKeys._
    import streamDecoderConf._
    Map[String,String](KEY_SCHEMA_RETENTION_POLICY -> retentionPolicy.toString,
      KEY_SCHEMA_REGISTRY_SETTINGS -> mapToKeyValueList(schemaRegistrySettings))
  }

  protected def convertToStreamWriterConfMap(streamWriterConf: ParquetStreamWriterConf): Map[String,String] = {
    import StreamWriterKeys._
    import streamWriterConf._
    val builder: Map[String,String] = extraConfOptions match {
      case Some(confs: Map[String,String]) => Map[String,String](KEY_EXTRA_CONFS -> mapToKeyValueList(confs))
      case None => Map[String,String]()
    }
    builder + (KEY_DESTINATION_DIRECTORY -> destination)
  }

  protected def convertToStreamTransformerConfMap(streamTransformerConf: ColumnSelectorStreamTransformerConf): Map[String,String] = {
    import StreamTransformerKeys._
    import streamTransformerConf._
    Map(KEY_TRANSFORMATION_QUERY -> columns.mkString(","))
  }

  protected def mapToKeyValueList(map: Map[String,String]): String = {
    map.foldLeft("") {
      case (acc, (key,value)) => acc + " " + key + "=" + value
    }
  }

  protected def equals(map1: Map[String,String], map2: Map[String,String]): Boolean = map1.toSet.diff(map2.toSet).isEmpty
}
