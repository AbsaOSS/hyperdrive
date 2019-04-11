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
import java.util.Properties

import org.apache.commons.io.FileUtils
import za.co.absa.hyperdrive.shared.utils.TempDir
import ConfigurationsKeys._
import StubConfigurationBeansFactory._

class TestPropertiesConfigurationsLoader extends BaseTestConfigurationsLoader {

  private val tempDir = TempDir.getNew
  private var propertiesFile: File = _

  override def afterAll(): Unit = FileUtils.deleteDirectory(tempDir)

  override def beforeEach(): Unit = {
    propertiesFile = getPropertiesFile
    initializeConfs()
    writePropertiesExcludingKeys(Set[String]())
  }
  override def afterEach(): Unit = FileUtils.cleanDirectory(tempDir)

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

    assert(expectedStreamTransformerConf.columns == actualConf.columns)
  }

  it should "not throw on no extra configurations for stream reader" in {
    beforeEach() // deletes the current properties file
    expectedStreamReaderConf = getStreamReaderConf // creates the configuration for the stream reader
    writePropertiesExcludingKeys(Set[String](StreamReaderKeys.KEY_EXTRA_CONFS)) // re-writes the properties file

    val actualConf = PropertiesConfigurationsLoader.load(propertiesFile.getAbsolutePath).streamReaderConf
    assert(expectedStreamReaderConf.topic == actualConf.topic)
    assert(expectedStreamReaderConf.brokers == actualConf.brokers)
    assert(actualConf.extraConfs.isEmpty)
  }

  it should "not throw on non-existent optional maps for stream writer" in {
    beforeEach()
    expectedStreamWriterConf = getStreamWriterWithoutExtraConf
    writePropertiesExcludingKeys(Set())

    val actualConf = PropertiesConfigurationsLoader.load(propertiesFile.getAbsolutePath).streamWriterConf

    assert(expectedStreamWriterConf.destination == actualConf.destination)
    assert(expectedStreamWriterConf.extraConfOptions.isDefined == actualConf.extraConfOptions.isDefined)
    assert(actualConf.extraConfOptions.isEmpty)
  }

  private def writePropertiesExcludingKeys(keysToExclude: Set[String]): Unit = {
    writeProperties(generatePropertiesFromConfs(keysToExclude))
  }

  private def writeProperties(properties: Properties): Unit = {
    val outStream = new FileOutputStream(propertiesFile)
    properties.store(outStream, "test properties")
    outStream.close()
  }

  private def getPropertiesFile: File = new File(tempDir, "Ingestion.properties")
}
