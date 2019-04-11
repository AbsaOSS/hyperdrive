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

import za.co.absa.hyperdrive.ingestor.configuration.loaders.ConfigurationsKeys.{StreamReaderKeys, StreamWriterKeys}
import za.co.absa.hyperdrive.ingestor.configuration.loaders.StubConfigurationBeansFactory.{getStreamReaderConf, getStreamWriterWithoutExtraConf}

class TestParametersConfigurationsLoader extends BaseTestConfigurationsLoader {

  private var args: Array[String] = _

  override def beforeEach(): Unit = {
    initializeConfs()
    val properties = generatePropertiesFromConfs(Set[String]())
    args = propertiesToArguments(properties)
  }

  behavior of ParametersConfigurationsLoader.getClass.getSimpleName

  it should "throw on empty arguments" in {
    assertThrows[IllegalArgumentException](ParametersConfigurationsLoader.load(Array[String]()))
  }

  it should "load Spark configurations" in {
    val actualConf = ParametersConfigurationsLoader.load(args).sparkConf

    assert(expectedSparkConf.appName == actualConf.appName)
  }

  it should "load stream reader configurations" in {
    val actualConf = ParametersConfigurationsLoader.load(args).streamReaderConf

    assert(expectedStreamReaderConf.topic == actualConf.topic)
    assert(expectedStreamReaderConf.brokers == actualConf.brokers)
    assert(expectedStreamReaderConf.extraConfs.size == actualConf.extraConfs.size)
    expectedStreamReaderConf.extraConfs.foreach {case (key,value) => assert(value == actualConf.extraConfs(key))}
  }

  it should "load offset manager configurations" in {
    val actualConf = ParametersConfigurationsLoader.load(args).offsetManagerConf

    assert(expectedOffsetManagerConf.topic == actualConf.topic)
    assert(expectedOffsetManagerConf.checkpointBaseLocation == actualConf.checkpointBaseLocation)
  }

  it should "load stream decoder configurations" in {
    val actualConf = ParametersConfigurationsLoader.load(args).streamDecoderConf

    assert(expectedStreamDecoderConf.retentionPolicy == actualConf.retentionPolicy)
    assert(equals(expectedStreamDecoderConf.schemaRegistrySettings, actualConf.schemaRegistrySettings))
  }

  it should "load stream writer configurations" in {
    val actualConf = ParametersConfigurationsLoader.load(args).streamWriterConf

    assert(expectedStreamWriterConf.destination == actualConf.destination)
    assert(expectedStreamWriterConf.extraConfOptions.isDefined == actualConf.extraConfOptions.isDefined)
    assert(equals(expectedStreamWriterConf.extraConfOptions.get, actualConf.extraConfOptions.get))
  }

  it should "load stream transformer configurations" in {
    val actualConf = ParametersConfigurationsLoader.load(args).streamTransformerConf

    assert(expectedStreamTransformerConf.columns == actualConf.columns)
  }

  it should "not throw on no extra configurations for stream reader" in {
    expectedStreamReaderConf = getStreamReaderConf // creates the configuration for the stream reader
    val properties = generatePropertiesFromConfs(Set[String](StreamReaderKeys.KEY_EXTRA_CONFS))
    val args = propertiesToArguments(properties)

    val actualConf = ParametersConfigurationsLoader.load(args).streamReaderConf
    assert(expectedStreamReaderConf.topic == actualConf.topic)
    assert(expectedStreamReaderConf.brokers == actualConf.brokers)
    assert(actualConf.extraConfs.isEmpty)
  }

  it should "not throw on non-existent optional maps for stream writer" in {
    expectedStreamWriterConf = getStreamWriterWithoutExtraConf
    val properties = generatePropertiesFromConfs(Set[String](StreamWriterKeys.KEY_EXTRA_CONFS))
    val args = propertiesToArguments(properties)

    val actualConf = ParametersConfigurationsLoader.load(args).streamWriterConf

    assert(expectedStreamWriterConf.destination == actualConf.destination)
    assert(expectedStreamWriterConf.extraConfOptions.isDefined == actualConf.extraConfOptions.isDefined)
    assert(actualConf.extraConfOptions.isEmpty)
  }

  private def propertiesToArguments(properties: Properties): Array[String] = {
    import scala.collection.JavaConverters._
    properties
      .propertyNames
        .asScala
        .map(propertyKey => s"$propertyKey=${formatMultipleArguments(properties.getProperty(propertyKey.toString))}")
      .toArray
  }

  private def formatMultipleArguments(argument: String): String = {
    argument
      .trim
      .replaceAll("^ +| +$|( )+",",")
      .replaceAll(",,", ",")
  }
}
