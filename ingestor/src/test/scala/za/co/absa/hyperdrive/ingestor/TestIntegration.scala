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

import java.io.File

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, Encoder, Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Matchers}
import org.springframework.kafka.test.EmbeddedKafkaBroker
import za.co.absa.abris.avro.read.confluent.SchemaManager
import za.co.absa.abris.avro.schemas.policy.SchemaRetentionPolicies
import za.co.absa.abris.avro.schemas.policy.SchemaRetentionPolicies.SchemaRetentionPolicy
import za.co.absa.hyperdrive.manager.offset.OffsetManager
import za.co.absa.hyperdrive.manager.offset.impl.CheckpointingOffsetManager
import za.co.absa.hyperdrive.reader.StreamReader
import za.co.absa.hyperdrive.reader.impl.KafkaStreamReader
import za.co.absa.hyperdrive.shared.InfrastructureSettings.{AvroSettings, HyperdriveSettings, KafkaSettings, SchemaRegistrySettings}
import za.co.absa.hyperdrive.shared.data.ComplexRecordsGenerator
import za.co.absa.hyperdrive.shared.utils.TempDir
import za.co.absa.hyperdrive.transformer.data.StreamTransformer
import za.co.absa.hyperdrive.transformer.data.impl.SelectAllStreamTransformer
import za.co.absa.hyperdrive.transformer.encoding.StreamDecoder
import za.co.absa.hyperdrive.transformer.encoding.impl.AvroStreamDecoder
import za.co.absa.hyperdrive.writer.StreamWriter
import za.co.absa.hyperdrive.writer.impl.ParquetStreamWriter

class TestIntegration extends FlatSpec with BeforeAndAfterAll with BeforeAndAfterEach with Matchers {

  private val tempDir = TempDir.getNew

  private val configuration = new Configuration
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
  private var decoder: AvroStreamDecoder = _
  private var transformer: StreamTransformer = _
  private var writer: StreamWriter = _

  override def afterAll(): Unit = kafkaBroker.destroy()

  override def beforeEach(): Unit = {
    setupDefaultParameters()
    setupDefaultInfrastructure()
    setupDefaultComponents()
  }

  override def afterEach(): Unit = {
    FileUtils.deleteDirectory(new File(ingestionDestination))
    FileUtils.deleteDirectory(new File(checkpointingLocation))
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
    reader = getKafkaStreamReader(defaultTopic, kafkaBroker.getBrokersAsString)
    manager = getCheckpointOffsetManager(defaultTopic, checkpointingLocation, configuration)
    decoder = getAvroDecoder(schemaRetentionPolicy, schemaRegistrySettings)
    transformer = getSelectAllTransformer
    writer = getParquetStreamWriter(ingestionDestination)
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

  private def getKafkaStreamReader(topic: String, brokers: String): StreamReader = new KafkaStreamReader(topic, brokers, Map[String,String]())

  private def getCheckpointOffsetManager(topic: String, checkpointLocation: String, conf: Configuration): OffsetManager = new CheckpointingOffsetManager(topic, checkpointLocation, conf)

  private def getAvroDecoder(retentionPolicy: SchemaRetentionPolicy, schemaRegistrySettings: Map[String,String]): AvroStreamDecoder = new AvroStreamDecoder(schemaRegistrySettings, retentionPolicy)

  private def getSelectAllTransformer: StreamTransformer = new SelectAllStreamTransformer

  private def getParquetStreamWriter(destination: String): StreamWriter = new ParquetStreamWriter(destination, None)

  private def produceRandomRecords(howMany: Int): List[Row] = ComplexRecordsGenerator.generateUnparsedRows(howMany)

  private def sendRecords(rows: List[Row], topic: String, brokers: String, schemaRegistrySettings: Map[String,String], spark: SparkSession)
                         (implicit encoder: Encoder[Row]): Unit = {
    import spark.implicits._
    val dataframe = spark.sparkContext.parallelize(rows, numSlices = 2).toDF()

    import za.co.absa.abris.avro.AvroSerDe._
    dataframe
      .toConfluentAvro(topic, AvroSettings.GENERAL_SCHEMA_NAME, AvroSettings.GENERAL_SCHEMA_NAMESPACE)(schemaRegistrySettings)
      .write
      .format(KafkaSettings.STREAM_FORMAT_KAFKA_NAME)
      .option(KafkaSettings.SPARK_BROKERS_SETTING_KEY, brokers)
      .option(KafkaSettings.TOPIC_DISPATCH_KEY, topic)
      .save()
  }

  private def readIngestedParquet(sourcePath: String, spark: SparkSession): DataFrame = spark.read.parquet(sourcePath)

  def getSchemaRegistrySettings(topic: String): Map[String,String] = {
    SchemaRegistrySettings.SCHEMA_REGISTRY_ACCESS_SETTINGS +
      (SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> topic) +
      (SchemaManager.PARAM_VALUE_SCHEMA_ID -> "latest")
  }

  private def equals(rows: List[Row], dataFrame: DataFrame)(implicit encoder: Encoder[Row]): Boolean = {
    val spark = dataFrame.sparkSession
    import spark.implicits._
    val that = spark.sparkContext.parallelize(rows).toDF

    val columns = dataFrame.schema.fields.map(_.name)
    val diff = columns.map(col => dataFrame.select(col).except(that.select(col)).count) // gets the difference by column
    diff.sum == 0
  }
}
