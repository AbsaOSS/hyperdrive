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

import java.io.File

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import org.apache.commons.configuration2.DynamicCombinedConfiguration
import org.apache.commons.io.FileUtils
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{DataFrame, Encoder, Row, SparkSession}
import org.scalatest._
import org.springframework.kafka.test.EmbeddedKafkaBroker
import za.co.absa.abris.avro.read.confluent.SchemaManager
import za.co.absa.abris.avro.read.confluent.SchemaManager.SchemaStorageNamingStrategies
import za.co.absa.abris.avro.schemas.policy.SchemaRetentionPolicies
import za.co.absa.abris.avro.schemas.policy.SchemaRetentionPolicies.SchemaRetentionPolicy
import za.co.absa.hyperdrive.decoder.StreamDecoder
import za.co.absa.hyperdrive.decoder.factories.StreamDecoderAbstractFactory
import za.co.absa.hyperdrive.manager.offset.OffsetManager
import za.co.absa.hyperdrive.manager.offset.factories.OffsetManagerAbstractFactory
import za.co.absa.hyperdrive.reader.StreamReader
import za.co.absa.hyperdrive.reader.factories.StreamReaderAbstractFactory
import za.co.absa.hyperdrive.shared.configurations.ConfigurationsKeys._
import za.co.absa.hyperdrive.shared.data.ComplexRecordsGenerator
import za.co.absa.hyperdrive.shared.utils.TempDir
import za.co.absa.hyperdrive.transformer.StreamTransformer
import za.co.absa.hyperdrive.transformer.factories.StreamTransformerAbstractFactory
import za.co.absa.hyperdrive.writer.StreamWriter
import za.co.absa.hyperdrive.writer.factories.StreamWriterAbstractFactory

@Ignore
class TestIntegration extends FlatSpec with BeforeAndAfterAll with BeforeAndAfterEach with Matchers {

  private val logger = LogManager.getLogger

  private val tempDir = TempDir.getNew

  private val configuration = new DynamicCombinedConfiguration()
  private val encoder = ComplexRecordsGenerator.getEncoder
  private var checkpointingLocation: String = _

  private var numberOfBrokers: Int = _
  private var numberOfPartitionsPerTopic: Int = _
  private var controlledShutdown: Boolean = _
  private var defaultTopic: String = _
  private var ingestionDestination: String = _
  private var schemaRetentionPolicy: SchemaRetentionPolicy = _
  private var kafkaBroker: EmbeddedKafkaBroker = _
  private var schemaRegistrySettings: Map[String,String] = _

  private var spark: SparkSession = _
  private var reader: StreamReader = _
  private var manager: OffsetManager = _
  private var decoder: StreamDecoder = _
  private var transformer: StreamTransformer = _
  private var writer: StreamWriter = _

  private lazy val SCHEMA_REGISTRY_ACCESS_SETTINGS = Map(
    SchemaManager.PARAM_SCHEMA_REGISTRY_URL          -> "http://localhost:8081",
    SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> SchemaStorageNamingStrategies.TOPIC_NAME,
    SchemaManager.PARAM_VALUE_SCHEMA_ID  -> "latest"
  )

  override def afterAll(): Unit = kafkaBroker.destroy()

  override def beforeEach(): Unit = {
    setupDefaultParameters()
    setupDefaultInfrastructure()
    setupSharedConfiguration()
    setupDefaultComponents()
  }

  override def afterEach(): Unit = {
    FileUtils.deleteDirectory(new File(ingestionDestination))
    FileUtils.deleteDirectory(new File(checkpointingLocation))
  }

  private def setupSharedConfiguration(): Unit = {
    configuration.clear()

    // Components
    configuration.addProperty(HyperdriveComponentsKeys.KEY_INGESTOR, "spark")
    configuration.addProperty(HyperdriveComponentsKeys.KEY_READER, StreamReaderAbstractFactory.getAvailableFactories.head)
    configuration.addProperty(HyperdriveComponentsKeys.KEY_MANAGER, OffsetManagerAbstractFactory.getAvailableFactories.head)
    configuration.addProperty(HyperdriveComponentsKeys.KEY_DECODER, StreamDecoderAbstractFactory.getAvailableFactories.head)
    configuration.addProperty(HyperdriveComponentsKeys.KEY_TRANSFORMER, StreamTransformerAbstractFactory.getAvailableFactories.head)
    configuration.addProperty(HyperdriveComponentsKeys.KEY_WRITER, StreamWriterAbstractFactory.getAvailableFactories.head)

    // StreamReader
    configuration.addProperty(KafkaStreamReaderKeys.KEY_TOPIC, defaultTopic)
    configuration.addProperty(KafkaStreamReaderKeys.KEY_BROKERS, kafkaBroker.getBrokersAsString)

    // OffsetManager
    configuration.addProperty(CheckpointOffsetManagerKeys.KEY_TOPIC, defaultTopic)
    configuration.addProperty(CheckpointOffsetManagerKeys.KEY_CHECKPOINT_BASE_LOCATION, checkpointingLocation)

    // StreamDecoder
    configuration.addProperty(AvroKafkaStreamDecoderKeys.KEY_TOPIC, defaultTopic)
    configuration.addProperty(AvroKafkaStreamDecoderKeys.KEY_SCHEMA_REGISTRY_URL, SCHEMA_REGISTRY_ACCESS_SETTINGS(SchemaManager.PARAM_SCHEMA_REGISTRY_URL))
    configuration.addProperty(AvroKafkaStreamDecoderKeys.KEY_SCHEMA_RETENTION_POLICY, schemaRetentionPolicy.toString)
    configuration.addProperty(AvroKafkaStreamDecoderKeys.KEY_SCHEMA_REGISTRY_VALUE_SCHEMA_ID, SCHEMA_REGISTRY_ACCESS_SETTINGS(SchemaManager.PARAM_VALUE_SCHEMA_ID))
    configuration.addProperty(AvroKafkaStreamDecoderKeys.KEY_SCHEMA_REGISTRY_VALUE_NAMING_STRATEGY, SCHEMA_REGISTRY_ACCESS_SETTINGS(SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY))

    // StreamTransformer
    configuration.addProperty(ColumnSelectorStreamTransformerKeys.KEY_COLUMNS_TO_SELECT, "*")

    // StreamWriter
    configuration.addProperty(ParquetStreamWriterKeys.KEY_DESTINATION_DIRECTORY, ingestionDestination)
  }

  private def setupDefaultParameters(): Unit = {
    numberOfBrokers = 2
    numberOfPartitionsPerTopic = 2
    controlledShutdown = false
    defaultTopic = "test_topic"
    ingestionDestination = new File(tempDir, "ingestionDestination").getAbsolutePath
    schemaRetentionPolicy = SchemaRetentionPolicies.RETAIN_SELECTED_COLUMN_ONLY
    schemaRegistrySettings = getSchemaRegistrySettings(defaultTopic)
    checkpointingLocation = {
      val checkpointDir = new File(tempDir, "checkpointingBaseDir")
      checkpointDir.mkdirs()
      checkpointDir.getAbsolutePath
    }
  }

  private def setupDefaultComponents(): Unit = {
    spark = getSparkSession
    reader = getStreamReader
    manager = getOffsetManager
    decoder = getStreamDecoder
    transformer = getStreamTransformer
    writer = getStreamWriter
  }

  private def setupDefaultInfrastructure(): Unit = {
    if (kafkaBroker == null) {
      kafkaBroker = new EmbeddedKafkaBroker(numberOfBrokers, controlledShutdown, numberOfPartitionsPerTopic, defaultTopic)
      kafkaBroker.afterPropertiesSet()
    }
    SchemaManager.setConfiguredSchemaRegistry(new MockSchemaRegistryClient)
  }

  behavior of SparkIngestor.getClass.getName

  it should "ingest from Kafka as Avro and save into Parquet" in {
    val records = produceRandomRecords(howMany = 1)
    ingest(defaultTopic, records)
    val ingested = readIngestedParquet(ingestionDestination, spark)
    assert(equals(records, ingested)(encoder))
  }

  private def ingest(topic: String, records: List[Row]): Unit = {
    sendRecords(records,
      topic,
      kafkaBroker.getBrokersAsString,
      schemaRegistrySettings,
      spark)(encoder)

    SparkIngestor.ingest(spark, reader, manager, decoder, transformer, writer)
  }

  private def getSparkSession: SparkSession = SparkSession.builder().master("local[*]").appName("IntegrationTest").getOrCreate()

  private def getStreamReader: StreamReader = StreamReaderAbstractFactory.build(configuration)

  private def getOffsetManager: OffsetManager = OffsetManagerAbstractFactory.build(configuration)

  private def getStreamDecoder: StreamDecoder = StreamDecoderAbstractFactory.build(configuration)

  private def getStreamTransformer: StreamTransformer = StreamTransformerAbstractFactory.build(configuration)

  private def getStreamWriter: StreamWriter = StreamWriterAbstractFactory.build(configuration)

  private def produceRandomRecords(howMany: Int): List[Row] = ComplexRecordsGenerator.generateUnparsedRows(howMany)

  private def sendRecords(rows: List[Row], topic: String, brokers: String, schemaRegistrySettings: Map[String,String], spark: SparkSession)
                         (implicit encoder: Encoder[Row]): Unit = {
    import spark.implicits._
    val dataframe = spark.sparkContext.parallelize(rows, numSlices = 2).toDF()

    import za.co.absa.abris.avro.AvroSerDe._
    dataframe
      .toConfluentAvro(topic, schemaName = "schemaName", schemaNamespace = "schemaNamespace")(schemaRegistrySettings)
      .write
      .format(source = "kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("topic", topic)
      .save()
  }

  private def readIngestedParquet(sourcePath: String, spark: SparkSession): DataFrame = spark.read.parquet(sourcePath)

  def getSchemaRegistrySettings(topic: String): Map[String,String] = {
    SCHEMA_REGISTRY_ACCESS_SETTINGS + (SchemaManager.PARAM_VALUE_SCHEMA_ID -> "latest")
  }

  private def equals(rows: List[Row], dataFrame: DataFrame)(implicit encoder: Encoder[Row]): Boolean = {
    val spark = dataFrame.sparkSession
    import spark.implicits._
    val that = spark.sparkContext.parallelize(rows).toDF

    val columns = dataFrame.schema.fields.map(_.name)
    if (columns.length == rows.head.length) {
      val diff = columns.map(col => dataFrame.select(col).except(that.select(col)).count) // gets the difference by column
      diff.sum == 0
    }
    else {
      logger.error(s"Mismatching number of columns: row has '${rows.head.length}', dataframe has '${columns.length}'.")
      false
    }
  }
}
