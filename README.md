    Copyright 2018 ABSA Group Limited
    
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

In Hyperdrive, each ingestion is defined by the four components reader, decoder, transformer and writer. This separation allows adapting to different streaming sources and sinks, while reusing transformations common across multiple ingestion pipelines.
## Motivation
Similar to batch processing, data ingestion pipelines are needed to process streaming data sources. While solutions for data pipelines exist, exactly-once fault-tolerance in streaming processing is an intricate problem and cannot be solved with the same strategies that exist for batch processing.

This is the gap the Hyperdrive aims to fill, by leveraging the exactly-once guarantee of Spark's Structured Streaming and by providing a flexible data pipeline. 

## Architecture
The data ingestion pipeline of Hyperdrive consists of four components: readers, decoders, transformers, writers.
- **Readers** define how to connect to sources, e.g. how to connect to Kafka in a secure cluster by providing security directives, which topic and brokers to connect to.
- **Decoders** define how to convert the payload into DataFrames, e.g. decoding from binary into Avro after retrieving the schema from schema registry. 
- **Transformers** define transformations to be applied to the decoded DataFrame, e.g. dropping columns.
- **Writers** define where DataFrames should be sent after the transformations, e.g. into HDFS as Parquet files.

### Built-in components
- `KafkaStreamReader` - reads from a Kafka topic.
- `ConfluentAvroKafkaStreamDecoder` - decodes the payload as Confluent Avro (through [ABRiS](https://github.com/AbsaOSS/ABRiS)), retrieving the schema from the specified Schema Registry. This decoder is capable of seamlessly handling whatever schemas the payload messages are using.
- `ColumnSelectorStreamTransformer` - selects all columns from the decoded DataFrame.
- `AddDateVersionTransformerStreamWriter` - adds columns for ingestion date and an auto-incremented version number, to be used for partitioning.
- `ParquetStreamWriter` - writes the DataFrame as Parquet, in **append** mode.
- `KafkaStreamWriter` - writes to a Kafka topic.

### Custom components
Custom components can be implemented using the [Component Archetype](component-archetype) following the API defined in the package `za.co.absa.hyperdrive.ingestor.api`
- A custom component has to be a class which extends either of the abstract classes `StreamReader`, `StreamDecoder`, `StreamTransformer` or `StreamWriter` 
- The class needs to have a companion object which implements the corresponding trait `StreamReaderFactory`, `StreamDecoderFactory`, `StreamTransformerFactory` or `StreamWriterFactory`
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
spark-submit --class za.co.absa.hyperdrive.driver.drivers.PropertiesIngestionDriver driver/target/driver*.jar config.properties
```

Alternatively, configuration properties can also be passed as command-line arguments
```
spark-submit --class za.co.absa.hyperdrive.driver.drivers.CommandLineIngestionDriver driver/target/driver*.jar \
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
| `component.decoder`     | Yes |  Fully qualified name of decoder component, e.g. `za.co.absa.hyperdrive.ingestor.implementation.decoder.avro.confluent.ConfluentAvroKafkaStreamDecoder` |
| `component.transformer.id.{order}` | No  | An arbitrary but unique string, referenced in this documentation as `{transformer-id}` |
| `component.transformer.class.{transformer-id}` | No  |  Fully qualified name of transformer component, e.g. `za.co.absa.hyperdrive.ingestor.implementation.transformer.column.selection.ColumnSelectorStreamTransformer` |
| `component.writer`      | Yes |  Fully qualified name of writer component, e.g. `za.co.absa.hyperdrive.ingestor.implementation.writer.parquet.ParquetStreamWriter` |

Multiple transformers can be configured in the pipeline, including multiple instances of the same transformer.
For each transformer instance, `component.transformer.id.{order}` and `component.transformer.class.{transformer-id}` have to specified, where `{order}` and `{transformer-id}` need to be unique.
In the above table, `{order}` must be an integer and may be negative. `{transformer-id}` is only used within the configuration
to identify which configuration options belong to a certain transformer instance. 

##### Spark settings
| Property Name | Required | Description |
| :--- | :---: | :--- |
| `ingestor.spark.app.name` | Yes | User-defined name of the Spark application. See Spark property `spark.app.name` |
| `ingestor.spark.termination.method` | No | Either `processAllAvailable` (stop query when no more messages are incoming) or `awaitTermination` (stop query on signal, e.g. Ctrl-C). Default: `processAllAvailable`. See also [Combination of trigger and termination method](#combination-of-trigger-and-termination-method) |
| `ingestor.spark.await.termination.timeout` | No | Timeout in milliseconds. Stops query when timeout is reached. This option is only valid with termination method `awaitTermination` |

#### Settings for built-in components
##### KafkaStreamReader
| Property Name | Required | Description |
| :--- | :---: | :--- |
| `reader.kafka.topic` | Yes | The name of the kafka topic to ingest data from. Equivalent to Spark property `subscribe` |
| `reader.kafka.brokers` | Yes | List of kafka broker URLs . Equivalent to Spark property `kafka.bootstrap.servers` |

Any additional properties for kafka can be added with the prefix `reader.option.`. E.g. the property `kafka.security.protocol` can be added as `reader.option.kafka.security.protocol`

See e.g. the [Structured Streaming + Kafka Integration Guide](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html) for optional kafka properties.

##### ConfluentAvroKafkaStreamDecoder
The `ConfluentAvroKafkaStreamDecoder` is built on [ABRiS](https://github.com/AbsaOSS/ABRiS). More details about the configuration properties can be found there.
**Caution**: The `ConfluentAvroKafkaStreamDecoder` requires the property `reader.kafka.topic` to be set.

| Property Name | Required | Description |
| :--- | :---: | :--- |
| `decoder.avro.schema.registry.url` | Yes | URL of Schema Registry, e.g. http://localhost:8081. Equivalent to ABRiS property `SchemaManager.PARAM_SCHEMA_REGISTRY_URL` |
| `decoder.avro.value.schema.id` | Yes | The schema id. Use `latest` or explicitly provide a number. Equivalent to ABRiS property `SchemaManager.PARAM_VALUE_SCHEMA_ID` |
| `decoder.avro.value.schema.naming.strategy` | Yes | Subject name strategy of Schema Registry. Possible values are `topic.name`, `record.name` or `topic.record.name`. Equivalent to ABRiS property `SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY` |
| `decoder.avro.value.schema.record.name` | Yes for naming strategies `record.name` and `topic.record.name` | Name of the record. Equivalent to ABRiS property `SchemaManager.PARAM_SCHEMA_NAME_FOR_RECORD_STRATEGY` |
| `decoder.avro.value.schema.record.namespace` | Yes for naming strategies `record.name` and `topic.record.name` | Namespace of the record. Equivalent to ABRiS property `SchemaManager.PARAM_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY` |
| `decoder.avro.consume.keys` | No | If set to `true`, keys will be consumed and added as columns to the dataframe. Key columns will be prefixed with `key__` |
| `decoder.avro.key.schema.id` | Yes if `decoder.avro.consume.keys` is true | The schema id for the key. |
| `decoder.avro.key.schema.naming.strategy` | Yes if `decoder.avro.consume.keys` is true | Subject name strategy for key |
| `decoder.avro.key.schema.record.name` | Yes for key naming strategies `record.name` and `topic.record.name` | Name of the record. |
| `decoder.avro.key.schema.record.namespace` | Yes for key naming strategies `record.name` and `topic.record.name` | Namespace of the record. |

For detailed information on the subject name strategy, please take a look at the [Schema Registry Documentation](https://docs.confluent.io/current/schema-registry/).

##### ColumnSelectorStreamTransformer
| Property Name | Required | Description |
| :--- | :---: | :--- |
| `transformer.{transformer-id}.columns.to.select` | Yes |  Comma-separated list of columns to select. `*` can be used to select all columns. Only existing columns using column names may be selected (i.e. expressions cannot be constructed) |

##### AddDateVersionTransformer
The `AddDateVersionTransformer` adds the columns `hyperdrive_date` and `hyperdrive_version`. `hyperdrive_date` is the ingestion date (or a user-defined date), while `hyperdrive_version` is a number automatically incremented with every ingestion, starting at 1.
For the auto-increment to work, `hyperdrive_date` and `hyperdrive_version` need to be defined as partition columns.

| Property Name | Required | Description |
| :--- | :---: | :--- |
| `transformer.{transformer-id}.destination.directory` | Yes | Destination path of the sink. Equivalent to Spark property `path` for the `DataStreamWriter` |
| `transformer.{transformer-id}.report.date` | No | User-defined date for `hyperdrive_date` in format `yyyy-MM-dd`. Default date is the date of the ingestion |

See [Pipeline settings](#pipeline-settings) for details about `{transformer-id}`.
##### ParquetStreamWriter
| Property Name | Required | Description |
| :--- | :---: | :--- |
| `writer.parquet.destination.directory` | Yes | Destination path of the sink. Equivalent to Spark property `path` for the `DataStreamWriter` |
| `writer.parquet.partition.columns` | No | Comma-separated list of columns to partition by. |
| `writer.parquet.metadata.check` | No | Set this option to `true` if the consistency of the metadata log should be checked prior to the query. For very large tables, the check may be very expensive |
| `writer.common.trigger.type` | No | See [Combination writer properties](#common-writer-properties) |
| `writer.common.trigger.processing.time` | No | See [Combination writer properties](#common-writer-properties) |

Any additional properties for the `DataStreamWriter` can be added with the prefix `writer.parquet.options`, e.g. `writer.parquet.options.key=value`

##### KafkaStreamWriter

| Property Name | Required | Description |
| :--- | :---: | :--- |
| `writer.kafka.topic` | Yes | The name of the kafka topic to ingest data from. Equivalent to Spark property `topic` |
| `writer.kafka.brokers` | Yes | List of kafka broker URLs . Equivalent to Spark property `kafka.bootstrap.servers` |
| `writer.kafka.schema.registry.url` | Yes | URL of Schema Registry, e.g. http://localhost:8081. Equivalent to ABRiS property `SchemaManager.PARAM_SCHEMA_REGISTRY_URL` |
| `writer.kafka.value.schema.naming.strategy` | Yes | Subject name strategy of Schema Registry. Possible values are `topic.name`, `record.name` or `topic.record.name`. Equivalent to ABRiS property `SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY` |
| `writer.kafka.value.schema.record.name` | Yes for naming strategies `record.name` and `topic.record.name` | Name of the record. Equivalent to ABRiS property `SchemaManager.PARAM_SCHEMA_NAME_FOR_RECORD_STRATEGY` |
| `writer.kafka.value.schema.record.namespace` | Yes for naming strategies `record.name` and `topic.record.name` | Namespace of the record. Equivalent to ABRiS property `SchemaManager.PARAM_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY` |
| `writer.kafka.produce.keys` | No | If set to `true`, keys will be produced according to the properties `key.column.prefix` and `key.column.names` of the [Hyperdrive Context](#hyperdrive-context) |
| `writer.kafka.key.schema.naming.strategy` | Yes if `writer.kafka.produce.keys` is true | Subject name strategy for the key |
| `writer.kafka.key.schema.record.name` | Yes if key naming strategy is either `record.name` or `topic.record.name` | Name of the record. |
| `writer.kafka.key.schema.record.namespace` | Yes if key naming strategy is either `record.name` or `topic.record.name` | Namespace of the record. |
| `writer.common.trigger.type` | No | See [Combination writer properties](#common-writer-properties) |
| `writer.common.trigger.processing.time` | No | See [Combination writer properties](#common-writer-properties) |

#### Common writer properties

| Property Name | Description |
| :--- | :--- | 
| `writer.common.checkpoint.location` | Yes | Used for Spark property `checkpointLocation`. The checkpoint location has to be unique among different workflows. |
| `writer.common.trigger.type` | Either `Once` for one-time execution or `ProcessingTime` for micro-batch executions for micro-batch execution. Default: `Once`. See also [Combination of trigger and termination method](#combination-of-trigger-and-termination-method) |
| `writer.common.trigger.processing.time` | Interval in ms for micro-batch execution (using `ProcessingTime`). Default: 0ms, i.e. execution as fast as possible. |

#### Combination of Trigger and termination method

| Trigger (`writer.common.trigger.type`) | Termination method (`ingestor.spark.termination.method`) | Runtime | Details |
| :--- | :--- | :--- | :--- |
| Once | AwaitTermination or ProcessAllAvailable | Limited | Consumes all data that is available at the beginning of the micro-batch. The query processes exactly one micro-batch and stops then, even if more data would be available at the end of the micro-batch. |
| Once | AwaitTermination with timeout | Limited | Same as above, but terminates at the timeout. If the timeout is reached before the micro-batch is processed, it won't be completed and no data will be committed. |
| ProcessingTime | ProcessAllAvailable | Only long-running if topic continuously produces messages, otherwise limited | Consumes all available data in micro-batches and only stops when no new data arrives, i.e. when the available offsets are the same as in the previous micro-batch. Thus, it completely depends on the topic, if and when the query terminates. |
| ProcessingTime | AwaitTermination with timeout | Limited | Consumes data in micro-batches and only stops when the timeout is reached or the query is killed. |
| ProcessingTime | AwaitTermination | Long-running | Consumes data in micro-batches and only stops when the query is killed. |

See the [Spark Documentation](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#triggers) for more information about triggers.

#### Hyperdrive Context
`HyperdriveContext` is an object intended to be used by the components to share data. It is a key-value store,
where the key is a string and the value can be of any type. The following context variables are currently used by the default implementation.

| Name | Type | Description |
| :--- | :--- | :--- |
| key.column.prefix | String | If `ConfluentAvroKafkaStreamDecoder` is configured to consume keys, it prefixes the key columns with `key__` such that they can be distinguished in the dataframe. If `key__` happens to be a prefix of a value column, a random alphanumeric string is used instead. |
| key.column.names | Seq[String] | If `ConfluentAvroKafkaStreamDecoder` is configured to consume keys, it contains the original column names (without prefix) in the key schema. |
 
#### Other
Hyperdrive uses [Apache Commons Configuration 2](https://github.com/apache/commons-configuration). This allows
properties to be referenced, e.g. like so
```
decoder.avro.schema.registry.url=http://localhost:8081
writer.kafka.schema.registry.url=${decoder.avro.schema.registry.url}
```

### Workflow Manager
Hyperdrive ingestions may be triggered using the Workflow Manager, which is developed in a separate repository: https://github.com/AbsaOSS/hyperdrive-trigger

A key feature of the Workflow Manager are triggers, which define when an ingestion should be executed and how it should be requested. The workflow manager supports cron-based triggers as well as triggers that listen to a notification topic.
