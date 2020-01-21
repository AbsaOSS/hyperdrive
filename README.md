    Copyright 2019 ABSA Group Limited
    
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
    
        http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

# Hyperdrive - An extensible streaming ingestion pipeline on top of Apache Spark

### Build Status
| master | develop |
| ------------- | ------------- |
| [![Build Status](https://opensource.bigusdatus.com/jenkins/buildStatus/icon?job=Absa-OSS-Projects%2Fhyperdrive%2Fmaster)](https://opensource.bigusdatus.com/jenkins/job/Absa-OSS-Projects/job/hyperdrive/job/master/) | [![Build Status](https://opensource.bigusdatus.com/jenkins/buildStatus/icon?job=Absa-OSS-Projects%2Fhyperdrive%2Fdevelop)](https://opensource.bigusdatus.com/jenkins/job/Absa-OSS-Projects/job/hyperdrive/job/develop/) | 

## What is Hyperdrive?

Hyperdrive is a configurable and scalable ingestion platform that allows data movement and transformation from streaming sources with exactly-once fault-tolerance semantics by using Apache Spark Structured Streaming.

In Hyperdrive, each ingestion is defined by the five components reader, manager, decoder, transformer and writer. This separation allows adapting to different streaming sources and sinks, while reusing transformations common across multiple ingestion pipelines.
## Motivation
Similar to batch processing, data ingestion pipelines are needed to process streaming data sources. While solutions for data pipelines exist, exactly-once fault-tolerance in streaming processing is an intricate problem and cannot be solved with the same strategies that exist for batch processing.

This is the gap the Hyperdrive aims to fill, by leveraging the exactly-once guarantee of Spark's Structured Streaming and by providing a flexible data pipeline. 

## Architecture
The data ingestion pipeline of Hyperdrive consists of five components: readers, managers, decoders, transformers, writers.
- **Readers** define how to connect to sources, e.g. how to connect to Kafka in a secure cluster by providing security directives, which topic and brokers to connect to.
- **Managers** define extra configurations for readers and writers, e.g. Kafka offset settings.
- **Decoders** define how to convert the payload into DataFrames, e.g. decoding from binary into Avro after retrieving the schema from schema registry. 
- **Transformers** define transformations to be applied to the decoded DataFrame, e.g. dropping columns.
- **Writers** define where DataFrames should be sent after the transformations, e.g. into HDFS as Parquet files.

### Built-in components
- `KafkaStreamReader` - connects to a secure Kafka broker.
- `CheckpointOffsetManager` - defines checkpoints for the stream reader and writer.
- `ConfluentAvroKafkaStreamDecoder` - decodes the payload as Confluent Avro (through [ABRiS](https://github.com/AbsaOSS/ABRiS)), retrieving the schema from the specified Schema Registry. This decoder is capable of seamlessly handling whatever schemas the payload messages are using.
- `ColumnSelectorStreamTransformer` - selects all columns from the decoded DataFrame.
- `ParquetStreamWriter` - writes the DataFrame as Parquet, in **append** mode, by invoking Spark's `processAllAvailable` method on the stream writer.
- `ParquetPartitioningStreamWriter` - writes the DataFrame as Parquet, partitioned by the ingestion date and an auto-incremented version number.

### Custom components
Custom components can be implemented using the [Component Archetype](component-archetype) following the API defined in the package `za.co.absa.hyperdrive.ingestor.api`
- A custom component has to be a class which extends either of the abstract classes `StreamReader`, `OffsetManager`, `StreamDecoder`, `StreamTransformer` or `StreamWriter` 
- The class needs to have a companion object which implements the corresponding trait `StreamReaderFactory`, `OffsetManagerFactory`, `StreamDecoderFactory`, `StreamTransformerFactory` or `StreamWriterFactory`
- The implemented components have to be packaged to a jar file, which can then be added to the classpath of the driver. To use a component, it has to be configured as described under [Usage](#usage)

After that, the new component will be able to be seamlessly invoked from the driver.

## Usage
Hyperdrive has to be executed with Spark. Due to Spark-Kafka integration issues, it will **only work with Spark 2.3 and higher**.

### How to run
```bash
git clone git@github.com:AbsaOSS/hyperdrive.git
mvn clean package
```

Given a configuration file has already been created, hyperdrive can be executed as follows:
```
spark-submit driver/target/driver*.jar config.properties
```

Alternatively, configuration properties can also be passed as command-line arguments
```
spark-submit driver/target/driver*.jar \
  component.ingestor=spark \
  component.reader=za.co.absa.hyperdrive.ingestor.implementation.reader.kafka.KafkaStreamReader \
  # more properties ...
```

### Configuration
The configuration file may be created from the template located at `driver/src/resources/Ingestion.properties.template`.

`CommandLineIngestionDriverDockerTest` may be consulted for a working pipeline configuration.

#### General settings
##### Pipeline settings
| Property Name | Required | Description |
| :--- | :---: | :--- |
| `component.ingestor`    | Yes |  Defines the ingestion pipeline. Only `spark` is currently supported. |
| `component.reader`      | Yes |  Fully qualified name of reader component, e.g.`za.co.absa.hyperdrive.ingestor.implementation.reader.kafka.KafkaStreamReader` |
| `component.manager`     | Yes |  Fully qualified name of manager component, e.g. `za.co.absa.hyperdrive.ingestor.implementation.manager.checkpoint.CheckpointOffsetManager` |
| `component.decoder`     | Yes |  Fully qualified name of decoder component, e.g. `za.co.absa.hyperdrive.ingestor.implementation.decoder.avro.confluent.ConfluentAvroKafkaStreamDecoder` |
| `component.transformer` | Yes |  Fully qualified name of transformer component, e.g. `za.co.absa.hyperdrive.ingestor.implementation.transformer.column.selection.ColumnSelectorStreamTransformer` |
| `component.writer`      | Yes |  Fully qualified name of writer component, e.g. `za.co.absa.hyperdrive.ingestor.implementation.writer.parquet.ParquetPartitioningStreamWriter` |

##### Spark settings
| Property Name | Required | Description |
| :--- | :---: | :--- |
| `ingestor.spark.app.name` | Yes | User-defined name of the Spark application. See Spark property `spark.app.name` |

#### Settings for built-in components
##### KafkaStreamReader
| Property Name | Required | Description |
| :--- | :---: | :--- |
| `reader.kafka.topic` | Yes | The name of the kafka topic to ingest data from. Equivalent to Spark property `subscribe` |
| `reader.kafka.brokers` | Yes | List of kafka broker URLs . Equivalent to Spark property `kafka.bootstrap.servers` |

Any additional properties for kafka can be added with the prefix `reader.option.`. E.g. the property `kafka.security.protocol` can be added as `reader.option.kafka.security.protocol`

See e.g. the [Structured Streaming + Kafka Integration Guide](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html) for optional kafka properties.

##### CheckpointOffsetManager
**Caution**: Currently, the `CheckpointOffsetManager` requires the property `reader.kafka.topic` to be set.

| Property Name | Required | Description |
| :--- | :---: | :--- |
| `manager.checkpoint.base.location` | Yes | Used for Spark property `checkpointLocation`. The checkpoint location is resolved to `{manager.checkpoint.base.location}/{topic}`, where `topic` is `reader.kafka.topic` |

##### ConfluentAvroKafkaStreamDecoder
The `ConfluentAvroKafkaStreamDecoder` is built on [ABRiS](https://github.com/AbsaOSS/ABRiS). More details about the configuration properties can be found there.

| Property Name | Required | Description |
| :--- | :---: | :--- |
| `decoder.avro.schema.registry.url` | Yes | URL of Schema Registry, e.g. http://localhost:8081. Equivalent to ABRiS property `SchemaManager.PARAM_SCHEMA_REGISTRY_URL` |
| `decoder.avro.value.schema.id` | Yes | The schema id. Use `latest` or explicitly provide a number. Equivalent to ABRiS property `SchemaManager.PARAM_VALUE_SCHEMA_ID` |
| `decoder.avro.value.schema.naming.strategy` | Yes | Subject name strategy of Schema Registry. Possible values are `topic.name`, `record.name` or `topic.record.name`. Equivalent to ABRiS property `SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY` |
| `decoder.avro.value.schema.record.name` | Yes for naming strategies `record.name` and `topic.record.name` | Name of the record. Equivalent to ABRiS property `SchemaManager.PARAM_SCHEMA_NAME_FOR_RECORD_STRATEGY` |
| `decoder.avro.value.schema.record.namespace` | Yes for naming strategies `record.name` and `topic.record.name` | Namespace of the record. Equivalent to ABRiS property `SchemaManager.PARAM_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY` |

For detailed information on the subject name strategy, please take a look at the [Schema Registry Documentation](https://docs.confluent.io/current/schema-registry/).

##### ColumnSelectorStreamTransformer
| Property Name | Required | Description |
| :--- | :---: | :--- |
| `transformer.columns.to.select` | Yes |  Comma-separated list of columns to select. `*` can be used to select all columns. Only existing columns using column names may be selected (i.e. expressions cannot be constructed) |

##### ParquetStreamWriter
| Property Name | Required | Description |
| :--- | :---: | :--- |
| `writer.parquet.destination.directory` | Yes | Destination path of the sink. Equivalent to Spark property `path` for the `DataStreamWriter` |

Any additional properties for the `DataStreamWriter` can be added with the prefix `writer.parquet.options`, e.g. `writer.parquet.options.key=value`

##### ParquetPartitioningStreamWriter
The `ParquetPartitioningStreamWriter` partitions every ingestion in the columns `hyperdrive_date` and `hyperdrive_version`. `hyperdrive_date` is the ingestion date (or a user-defined date), while `hyperdrive_version` is a number automatically incremented with every ingestion, starting at 1.

| Property Name | Required | Description |
| :--- | :---: | :--- |
| `writer.parquet.destination.directory` | Yes | Destination path of the sink. Equivalent to Spark property `path` for the `DataStreamWriter` |
| `writer.parquet.partitioning.report.date` | No | User-defined date for `hyperdrive_date` in format `yyyy-MM-dd`. Default date is the date of the ingestion |

Any additional properties for the `DataStreamWriter` can be added with the prefix `writer.parquet.options`, e.g. `writer.parquet.options.key=value`

### Workflow Manager
Hyperdrive ingestions may be triggered using the Workflow Manager, which is developed in a separate repository: https://github.com/AbsaOSS/hyperdrive-trigger

A key feature of the Workflow Manager are triggers, which define when an ingestion should be executed and how it should be requested. The workflow manager supports cron-based triggers as well as triggers that listen to a notification topic.
