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

In Hyperdrive, each ingestion is defined by the three components reader, transformer and writer. This separation allows adapting to different streaming sources and sinks, while reusing transformations common across multiple ingestion pipelines.
## Motivation
Similar to batch processing, data ingestion pipelines are needed to process streaming data sources. While solutions for data pipelines exist, exactly-once fault-tolerance in streaming processing is an intricate problem and cannot be solved with the same strategies that exist for batch processing.

This is the gap the Hyperdrive aims to fill, by leveraging the exactly-once guarantee of Spark's Structured Streaming and by providing a flexible data pipeline. 

## Architecture
The data ingestion pipeline of Hyperdrive consists of four components: readers, transformers, writers.
- **Readers** define how to connect to sources, e.g. how to connect to Kafka in a secure cluster by providing security directives, which topic and brokers to connect to.
- **Transformers** define transformations to be applied to the decoded DataFrame, e.g. dropping columns.
- **Writers** define where DataFrames should be sent after the transformations, e.g. into HDFS as Parquet files.

### Built-in components
- `KafkaStreamReader` - reads from a Kafka topic.
- `ParquetStreamReader` - reads Parquet files from a source directory.
- `ConfluentAvroDecodingTransformer` - decodes the payload as Confluent Avro (through [ABRiS](https://github.com/AbsaOSS/ABRiS)), retrieving the schema from the specified Schema Registry. This transformer is capable of seamlessly handling whatever schemas the payload messages are using.
- `ConfluentAvroEncodingTransformer` - encodes the payload as Confluent Avro (through [ABRiS](https://github.com/AbsaOSS/ABRiS)), updating the schema to the specified Schema Registry. This transformer is capable of seamlessly handling whatever schema the dataframe is using.
- `ColumnSelectorStreamTransformer` - selects all columns from the decoded DataFrame.
- `AddDateVersionTransformerStreamWriter` - adds columns for ingestion date and an auto-incremented version number, to be used for partitioning.
- `ParquetStreamWriter` - writes the DataFrame as Parquet, in **append** mode.
- `KafkaStreamWriter` - writes to a Kafka topic.
- `DeltaCDCToSnapshotWriter` - writes the DataFrame in Delta format. It expects CDC events and performs merge logic and creates the latest snapshot table.
- `DeltaCDCToSCD2Writer` - writes the DataFrame in Delta format. It expects CDC events and performs merge logic and creates SCD2 table.
- `HudiCDCToSCD2Writer` - writes the DataFrame in Hudi format. It expects CDC events and performs merge logic and creates SCD2 table.

### Custom components
Custom components can be implemented using the [Component Archetype](component-archetype) following the API defined in the package `za.co.absa.hyperdrive.ingestor.api`
- A custom component has to be a class which extends either of the abstract classes `StreamReader`, `StreamTransformer` or `StreamWriter` 
- The class needs to have a companion object which implements the corresponding trait `StreamReaderFactory`, `StreamTransformerFactory` or `StreamWriterFactory`
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
| `ingestor.spark.termination.method` | No | Either `processAllAvailable` (stop query when no more messages are incoming) or `awaitTermination` (stop query on signal, e.g. Ctrl-C). Default: `awaitTermination`. See also [Combination of trigger and termination method](#combination-of-trigger-and-termination-method) |
| `ingestor.spark.await.termination.timeout` | No | Timeout in milliseconds. Stops query when timeout is reached. This option is only valid with termination method `awaitTermination` |

#### Settings for built-in components
##### KafkaStreamReader
| Property Name | Required | Description |
| :--- | :---: | :--- |
| `reader.kafka.topic` | Yes | The name of the kafka topic to ingest data from. Equivalent to Spark property `subscribe` |
| `reader.kafka.brokers` | Yes | List of kafka broker URLs . Equivalent to Spark property `kafka.bootstrap.servers` |

Any additional properties for kafka can be added with the prefix `reader.option.`. E.g. the property `kafka.security.protocol` can be added as `reader.option.kafka.security.protocol`

See e.g. the [Structured Streaming + Kafka Integration Guide](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html) for optional kafka properties.

##### ParquetStreamReader
The parquet stream reader infers the schema from parquet files that already exist in the source directory. 
If no file exists, the reader will fail.

| Property Name | Required | Description |
| :--- | :---: | :--- |
| `reader.parquet.source.directory` | Yes | Source path for the parquet files. Equivalent to Spark property `path` for the `DataStreamReader` |

Any additional properties can be added with the prefix `reader.parquet.options.`. See [Spark Structured Streaming Documentation](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#input-sources)

##### ConfluentAvroDecodingTransformer
The `ConfluentAvroDecodingTransformer` is built on [ABRiS](https://github.com/AbsaOSS/ABRiS). More details about the configuration properties can be found there.
**Caution**: The `ConfluentAvroDecodingTransformer` requires the property `reader.kafka.topic` to be set.

| Property Name | Required | Description |
| :--- | :---: | :--- |
| `transformer.{transformer-id}.schema.registry.url` | Yes | URL of Schema Registry, e.g. http://localhost:8081. Equivalent to ABRiS property `SchemaManager.PARAM_SCHEMA_REGISTRY_URL` |
| `transformer.{transformer-id}.value.schema.id` | Yes | The schema id. Use `latest` or explicitly provide a number. Equivalent to ABRiS property `SchemaManager.PARAM_VALUE_SCHEMA_ID` |
| `transformer.{transformer-id}.value.schema.naming.strategy` | Yes | Subject name strategy of Schema Registry. Possible values are `topic.name`, `record.name` or `topic.record.name`. Equivalent to ABRiS property `SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY` |
| `transformer.{transformer-id}.value.schema.record.name` | Yes for naming strategies `record.name` and `topic.record.name` | Name of the record. Equivalent to ABRiS property `SchemaManager.PARAM_SCHEMA_NAME_FOR_RECORD_STRATEGY` |
| `transformer.{transformer-id}.value.schema.record.namespace` | Yes for naming strategies `record.name` and `topic.record.name` | Namespace of the record. Equivalent to ABRiS property `SchemaManager.PARAM_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY` |
| `transformer.{transformer-id}.consume.keys` | No | If set to `true`, keys will be consumed and added as columns to the dataframe. Key columns will be prefixed with `key__` |
| `transformer.{transformer-id}.key.schema.id` | Yes if `consume.keys` is true | The schema id for the key. |
| `transformer.{transformer-id}.key.schema.naming.strategy` | Yes if `consume.keys` is true | Subject name strategy for key |
| `transformer.{transformer-id}.key.schema.record.name` | Yes for key naming strategies `record.name` and `topic.record.name` | Name of the record. |
| `transformer.{transformer-id}.key.schema.record.namespace` | Yes for key naming strategies `record.name` and `topic.record.name` | Namespace of the record. |
| `transformer.{transformer-id}.keep.columns` | No | Comma-separated list of columns to keep (e.g. offset, partition) |
| `transformer.{transformer-id}.disable.nullability.preservation` | No | Set to true to ignore fix [#137](https://github.com/AbsaOSS/hyperdrive/issues/137) and to keep the same behaviour as for versions prior to and including v3.2.2. Default value: `false` |
| `transformer.{transformer-id}.schema.registry.basic.auth.user.info.file` | No | A path to a text file, that contains one line in the form `<username>:<password>`. It will be passed as `basic.auth.user.info` to the schema registry config |
| `transformer.{transformer-id}.use.advanced.schema.conversion` | No | Set to true to convert the avro schema using [AdvancedAvroToSparkConverter](https://github.com/AbsaOSS/hyperdrive/blob/develop/ingestor-default/src/main/scala/za/co/absa/hyperdrive/ingestor/implementation/transformer/avro/confluent/AdvancedAvroToSparkConverter.scala), which puts default value and underlying avro type to struct field metadata. Default false |

For detailed information on the subject name strategy, please take a look at the [Schema Registry Documentation](https://docs.confluent.io/current/schema-registry/).

Any additional properties for the schema registry config can be added with the prefix `transformer.{transformer-id}.schema.registry.options.`

Note: `use.advanced.schema.conversion` only works with a patched version of Spark, due to bug [SPARK-34805](https://issues.apache.org/jira/browse/SPARK-34805). 
For the latest version of Spark, the patch is available in https://github.com/apache/spark/pull/35270. For other versions of Spark, the changes need to be cherry-picked and built locally.

##### ConfluentAvroEncodingTransformer
The `ConfluentAvroEncodingTransformer` is built on [ABRiS](https://github.com/AbsaOSS/ABRiS). More details about the configuration properties can be found there.
**Caution**: The `ConfluentAvroEncodingTransformer` requires the property `writer.kafka.topic` to be set.

| Property Name | Required | Description |
| :--- | :---: | :--- |
| `transformer.{transformer-id}.schema.registry.url` | Yes | URL of Schema Registry, e.g. http://localhost:8081. Equivalent to ABRiS property `SchemaManager.PARAM_SCHEMA_REGISTRY_URL` |
| `transformer.{transformer-id}.value.schema.naming.strategy` | Yes | Subject name strategy of Schema Registry. Possible values are `topic.name`, `record.name` or `topic.record.name`. Equivalent to ABRiS property `SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY` |
| `transformer.{transformer-id}.value.schema.record.name` | Yes for naming strategies `record.name` and `topic.record.name` | Name of the record. Equivalent to ABRiS property `SchemaManager.PARAM_SCHEMA_NAME_FOR_RECORD_STRATEGY` |
| `transformer.{transformer-id}.value.schema.record.namespace` | Yes for naming strategies `record.name` and `topic.record.name` | Namespace of the record. Equivalent to ABRiS property `SchemaManager.PARAM_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY` |
| `transformer.{transformer-id}.value.optional.fields` | No | Comma-separated list of nullable value columns that should get default value null in the avro schema. Nested columns' names should be concatenated with the dot (`.`) |
| `transformer.{transformer-id}.produce.keys` | No | If set to `true`, keys will be produced according to the properties `key.column.prefix` and `key.column.names` of the [Hyperdrive Context](#hyperdrive-context) |
| `transformer.{transformer-id}.key.schema.naming.strategy` | Yes if `produce.keys` is true | Subject name strategy for key |
| `transformer.{transformer-id}.key.schema.record.name` | Yes for key naming strategies `record.name` and `topic.record.name` | Name of the record. |
| `transformer.{transformer-id}.key.schema.record.namespace` | Yes for key naming strategies `record.name` and `topic.record.name` | Namespace of the record. |
| `transformer.{transformer-id}.key.optional.fields` | No | Comma-separated list of nullable key columns that should get default value null in the avro schema. Nested columns' names should be concatenated with the dot (`.`) |
| `transformer.{transformer-id}.schema.registry.basic.auth.user.info.file` | No | A path to a text file, that contains one line in the form `<username>:<password>`. It will be passed as `basic.auth.user.info` to the schema registry config |
| `transformer.{transformer-id}.use.advanced.schema.conversion` | No | Set to true to convert the avro schema using [AdvancedSparkToAvroConverter](https://github.com/AbsaOSS/hyperdrive/blob/develop/ingestor-default/src/main/scala/za/co/absa/hyperdrive/ingestor/implementation/transformer/avro/confluent/AdvancedSparkToAvroConverter.scala), which reads default value and underlying avro type from struct field metadata. Default false |

Any additional properties for the schema registry config can be added with the prefix `transformer.{transformer-id}.schema.registry.options.`

##### ColumnSelectorStreamTransformer
| Property Name | Required | Description |
| :--- | :---: | :--- |
| `transformer.{transformer-id}.columns.to.select` | Yes |  Comma-separated list of columns to select. `*` can be used to select all columns. Only existing columns using column names may be selected (i.e. expressions cannot be constructed) |

##### AddDateVersionTransformer
The `AddDateVersionTransformer` adds the columns `hyperdrive_date` and `hyperdrive_version`. `hyperdrive_date` is the ingestion date (or a user-defined date), while `hyperdrive_version` is a number automatically incremented with every ingestion, starting at 1.
For the auto-increment to work, `hyperdrive_date` and `hyperdrive_version` need to be defined as partition columns.
Caution: This transformer requires a writer which defines `writer.parquet.destination.directory`.

| Property Name | Required | Description |
| :--- | :---: | :--- |
| `transformer.{transformer-id}.report.date` | No | User-defined date for `hyperdrive_date` in format `yyyy-MM-dd`. Default date is the date of the ingestion |

##### ColumnRenamingStreamTransformer
`ColumnRenamingStreamTransformer` allows renaming of columns specified in the configuration.

To add the transformer to the pipeline use this class name:
```
component.transformer.class.{transformer-id} = za.co.absa.hyperdrive.ingestor.implementation.transformer.column.renaming.ColumnRenamingStreamTransformer
```

| Property Name | Required | Description |
| :--- | :---: | :--- |
| `transformer.{transformer-id}.columns.rename.from` | Yes | A comma-separated list of columns to rename. For example, `column1, column2`. |
| `transformer.{transformer-id}.columns.rename.to` | Yes | A comma-separated list of new column names. For example, `column1_new, column2_new`. |

##### ColumnCopyStreamTransformer
`ColumnCopyStreamTransformer` allows copying of columns specified in the configuration. Dots in column names are interpreted as nested structs, 
unless they are surrounded by backticks (same as Spark convention)

Note that usage of the star-operator `*` within column names is not supported and may lead to unexpected behaviour.

To add the transformer to the pipeline use this class name:
```
component.transformer.class.{transformer-id} = za.co.absa.hyperdrive.ingestor.implementation.transformer.column.copy.ColumnCopyStreamTransformer
```

| Property Name | Required | Description |
| :--- | :---: | :--- |
| `transformer.{transformer-id}.columns.copy.from` | Yes | A comma-separated list of columns to copy from. For example, `column1.fieldA, column2.fieldA`. |
| `transformer.{transformer-id}.columns.copy.to` | Yes | A comma-separated list of new column names. For example, `newColumn.col1_fieldA, newColumn.col2_fieldA`. |

**Example**

Given a dataframe with the following schema
```
 |-- column1
 |    |-- fieldA
 |    |-- fieldB
 |-- column2
 |    |-- fieldA
 |-- column3
```

Then, the following column parameters
- `transformer.{transformer-id}.columns.copy.from=column1.fieldA, column2.fieldA`
- `transformer.{transformer-id}.columns.copy.to=newColumn.col1_fieldA, newColumn.col2_fieldA`

will produce the following schema
```
 |-- column1
 |    |-- fieldA
 |    |-- fieldB
 |-- column2
 |    |-- fieldA
 |-- column3
 |-- newColumn
 |    |-- col1_fieldA
 |    |-- col2_fieldA

```


##### DeduplicateKafkaSinkTransformer
`DeduplicateKafkaSinkTransformer` deduplicates records in a query from a Kafka source to a Kafka destination in a rerun after a failure.
Records are identified across source and destination topic by a user-defined id, which may be a composite id and may include consumer record
properties such as offset, partition, but also fields from the key or value schema.
Deduplication is needed because the Kafka-destination provides only a at-least-once guarantee. Deduplication works by getting the ids
from the last partial run in the destination topic and excluding them in the query. 

Note that there must be only one source and one destination topic, and there must be only one writer writing to the destination topic, and
no records must have been written to the destination topic after the partial run. Otherwise, records may still be duplicated.

To use this transformer, `KafkaStreamReader`, `ConfluentAvroDecodingTransformer`, `ConfluentAvroEncodingTransformer` and `KafkaStreamWriter`
must be configured as well.

Note that usage of the star-operator `*` within column names is not supported and may lead to unexpected behaviour.

To add the transformer to the pipeline use this class name:
```
component.transformer.class.{transformer-id} = za.co.absa.hyperdrive.ingestor.implementation.transformer.deduplicate.kafka.DeduplicateKafkaSinkTransformer
```

| Property Name | Required | Description |
| :--- | :---: | :--- |
| `transformer.{transformer-id}.source.id.columns` | Yes | A comma-separated list of consumer record properties that define the composite id. For example, `offset, partition` or `key.some_user_id`. |
| `transformer.{transformer-id}.destination.id.columns` | Yes | A comma-separated list of consumer record properties that define the composite id. For example, `value.src_offset, value.src_partition` or `key.some_user_id`. |
| `transformer.{transformer-id}.kafka.consumer.timeout` | No | Kafka consumer timeout in seconds. The default value is 120s. |

The following fields can be selected on the consumer record

- `topic`
- `offset`
- `partition`
- `timestamp`
- `timestampType`
- `serializedKeySize`
- `serializedValueSize`
- `key`
- `value`

In case of `key` and `value`, the fields of their schemas can be specified by adding a dot, e.g.
`key.some_nested_record.some_id` or likewise `value.some_nested_record.some_id`

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
| `writer.common.trigger.type` | No | See [Combination writer properties](#common-writer-properties) |
| `writer.common.trigger.processing.time` | No | See [Combination writer properties](#common-writer-properties) |

##### MongoDbStreamWriter
| Property Name | Required | Description |
| :--- | :---: | :--- |
| `writer.mongodb.uri` | Yes | Output MongoDB URI, e.g. `mongodb://host:port/database.collection`. |
| `writer.mongodb.database` | No | Database name (if not specified as the part of URI). |
| `writer.mongodb.collection` | No | Collection name (if not specified as the part of URI). |
| `writer.common.trigger.type` | No | See [Combination writer properties](#common-writer-properties) |
| `writer.common.trigger.processing.time` | No | See [Combination writer properties](#common-writer-properties) |

Any additional properties for the `DataStreamWriter` can be added with the prefix `writer.mongodb.options`, e.g. `writer.mongodb.options.key=value`

Common MongoDB additional options

| Property Name | Default | Description |
| :--- | :--- | :--- |
| `writer.mongodb.option.spark.mongodb.output.ordered` | `true` | When set to `false` inserts are done in parallel, increasing performance, but the order of documents is not preserved. |
| `writer.mongodb.option.spark.mongodb.output.forceInsert` | `false`| Forces saves to use inserts, even if a Dataset contains `_id.` |
More on these options: https://docs.mongodb.com/spark-connector/current/configuration

##### DeltaCDCToSnapshotWriter
| Property Name                                             | Required | Description                                                                                                                                |
|:----------------------------------------------------------| :---: |:-------------------------------------------------------------------------------------------------------------------------------------------|
| `writer.deltacdctosnapshot.destination.directory`         | Yes | Destination path of the sink. Equivalent to Spark property `path` for the `DataStreamWriter`                                               |
| `writer.deltacdctosnapshot.partition.columns`             | No | Comma-separated list of columns to partition by.                                                                                           |
| `writer.deltacdctosnapshot.key.column`                    | Yes | A column with unique entity identifier.                                                                                                    |
| `writer.deltacdctosnapshot.operation.column`              | Yes | A column containing value marking a record with an operation.                                                                              |
| `writer.deltacdctosnapshot.operation.deleted.values`      | Yes | Values marking a record for deletion in the operation column.                                                                              |
| `writer.deltacdctosnapshot.precombineColumns`             | Yes | When two records have the same key value, we will pick the one with the largest value for precombine columns. Evaluated in provided order. |
| `writer.deltacdctosnapshot.precombineColumns.customOrder` | No | Precombine column's custom order in ascending order.                                                                                       |
| `writer.common.trigger.type`                              | No | See [Combination writer properties](#common-writer-properties)                                                                             |
| `writer.common.trigger.processing.time`                   | No | See [Combination writer properties](#common-writer-properties)                                                                             |

Any additional properties for the `DataStreamWriter` can be added with the prefix `writer.deltacdctosnapshot.options`, e.g. `writer.deltacdctosnapshot.options.key=value`

**Example**

- `component.writer=za.co.absa.hyperdrive.compatibility.impl.writer.cdc.delta.snapshot.DeltaCDCToSnapshotWriter`
- `writer.deltacdctosnapshot.destination.directory=/tmp/destination`
- `writer.deltacdctosnapshot.key.column=key`
- `writer.deltacdctosnapshot.operation.column=ENTTYP`
- `writer.deltacdctosnapshot.operation.deleted.values=DL,FD`
- `writer.deltacdctosnapshot.precombineColumns=TIMSTAMP, ENTTYP`
- `writer.deltacdctosnapshot.precombineColumns.customOrder.ENTTYP=PT,FI,RR,UB,UP,DL,FD`

##### DeltaCDCToSCD2Writer
| Property Name                                         | Required | Description                                                                                                                                              |
|:------------------------------------------------------| :---: |:---------------------------------------------------------------------------------------------------------------------------------------------------------|
| `writer.deltacdctoscd2.destination.directory`         | Yes | Destination path of the sink. Equivalent to Spark property `path` for the `DataStreamWriter`                                                             |
| `writer.deltacdctoscd2.partition.columns`             | No | Comma-separated list of columns to partition by.                                                                                                         |
| `writer.deltacdctoscd2.key.column`                    | Yes | A column with unique entity identifier.                                                                                                                  |
| `writer.deltacdctoscd2.timestamp.column`              | Yes | A column with timestamp.                                                                                                                                 |
| `writer.deltacdctoscd2.operation.column`              | Yes | A column containing value marking a record with an operation.                                                                                            |
| `writer.deltacdctoscd2.operation.deleted.values`      | Yes | Values marking a record for deletion in the operation column.                                                                                            |
| `writer.deltacdctoscd2.precombineColumns`             | Yes | When two records have the same key and timestamp value, we will pick the one with the largest value for precombine columns. Evaluated in provided order. |
| `writer.deltacdctoscd2.precombineColumns.customOrder` | No | Precombine column's custom order in ascending order.                                                                                                     |
| `writer.common.trigger.type`                          | No | See [Combination writer properties](#common-writer-properties)                                                                                           |
| `writer.common.trigger.processing.time`               | No | See [Combination writer properties](#common-writer-properties)                                                                                           |

Any additional properties for the `DataStreamWriter` can be added with the prefix `writer.deltacdctoscd2.options`, e.g. `writer.deltacdctoscd2.options.key=value`

**Example**
- `component.writer=za.co.absa.hyperdrive.compatibility.impl.writer.cdc.delta.scd2.DeltaCDCToSCD2Writer`
- `writer.deltacdctoscd2.destination.directory=/tmp/destination`
- `writer.deltacdctoscd2.key.column=key`
- `writer.deltacdctoscd2.timestamp.column=TIMSTAMP`
- `writer.deltacdctoscd2.operation.column=ENTTYP`
- `writer.deltacdctoscd2.operation.deleted.values=DL,FD`
- `writer.deltacdctoscd2.precombineColumns=ENTTYP`
- `writer.deltacdctoscd2.precombineColumns.customOrder.ENTTYP=PT,FI,RR,UB,UP,DL,FD`

##### HudiCDCToSCD2Writer
| Property Name                                         | Required | Description                                                                                                                                              |
|:------------------------------------------------------| :---: |:---------------------------------------------------------------------------------------------------------------------------------------------------------|
| `writer.hudicdctoscd2.destination.directory`         | Yes | Destination path of the sink. Equivalent to Spark property `path` for the `DataStreamWriter`                                                             |
| `writer.hudicdctoscd2.partition.columns`             | No | Comma-separated list of columns to partition by.                                                                                                         |
| `writer.hudicdctoscd2.key.column`                    | Yes | A column with unique entity identifier.                                                                                                                  |
| `writer.hudicdctoscd2.timestamp.column`              | Yes | A column with timestamp.                                                                                                                                 |
| `writer.hudicdctoscd2.operation.column`              | Yes | A column containing value marking a record with an operation.                                                                                            |
| `writer.hudicdctoscd2.operation.deleted.values`      | Yes | Values marking a record for deletion in the operation column.                                                                                            |
| `writer.hudicdctoscd2.precombineColumns`             | Yes | When two records have the same key and timestamp value, we will pick the one with the largest value for precombine columns. Evaluated in provided order. |
| `writer.hudicdctoscd2.precombineColumns.customOrder` | No | Precombine column's custom order in ascending order.                                                                                                     |
| `writer.common.trigger.type`                          | No | See [Combination writer properties](#common-writer-properties)                                                                                           |
| `writer.common.trigger.processing.time`               | No | See [Combination writer properties](#common-writer-properties)                                                                                           |

Any additional properties for the `DataStreamWriter` can be added with the prefix `writer.hudicdctoscd2.options`, e.g. `writer.hudicdctoscd2.options.key=value`

**Example**
- `component.writer=za.co.absa.hyperdrive.compatibility.impl.writer.cdc.hudi.scd2.HudiCDCToSCD2Writer`
- `writer.hudicdctoscd2.destination.directory=/tmp/destination`
- `writer.hudicdctoscd2.key.column=key`
- `writer.hudicdctoscd2.timestamp.column=TIMSTAMP`
- `writer.hudicdctoscd2.operation.column=ENTTYP`
- `writer.hudicdctoscd2.operation.deleted.values=DL,FD`
- `writer.hudicdctoscd2.precombineColumns=ENTTYP`
- `writer.hudicdctoscd2.precombineColumns.customOrder.ENTTYP=PT,FI,RR,UB,UP,DL,FD`

#### Common writer properties

| Property Name | Required |Description |
| :--- | :---: | :--- |
| `writer.common.checkpoint.location` | Yes | Used for Spark property `checkpointLocation`. The checkpoint location has to be unique among different workflows. |
| `writer.common.trigger.type` | No | Either `Once` for one-time execution or `ProcessingTime` for micro-batch executions for micro-batch execution. Default: `Once`. |
| `writer.common.trigger.processing.time` | No | Interval in ms for micro-batch execution (using `ProcessingTime`). Default: 0ms, i.e. execution as fast as possible. |

#### Behavior of Triggers

| Trigger (`writer.common.trigger.type`) | Termination method (`ingestor.spark.termination.method`) | Runtime | Details |
| :--- | :--- | :--- | :--- |
| Once | AwaitTermination or ProcessAllAvailable | Limited | Consumes all data that is available at the beginning of the micro-batch. The query processes exactly one micro-batch and stops then, even if more data would be available at the end of the micro-batch. |
| Once | AwaitTermination with timeout | Limited | Same as above, but terminates at the timeout. If the timeout is reached before the micro-batch is processed, it won't be completed and no data will be committed. |
| ProcessingTime | ProcessAllAvailable | Only long-running if topic continuously produces messages, otherwise limited | Consumes all available data in micro-batches and only stops when no new data arrives, i.e. when the available offsets are the same as in the previous micro-batch. Thus, it completely depends on the topic, if and when the query terminates. |
| ProcessingTime | AwaitTermination with timeout | Limited | Consumes data in micro-batches and only stops when the timeout is reached or the query is killed. |
| ProcessingTime | AwaitTermination | Long-running | Consumes data in micro-batches and only stops when the query is killed. |

- Note 1: The first micro-batch of the query will contain all available messages to consume and can therefore be quite large,
 even if the trigger `ProcessingTime` is configured, and regardless of what micro-batch interval is configured.
 To limit the size of a micro-batch, the property `reader.option.maxOffsetsPerTrigger` should be used. See also http://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
- Note 2: It's possible to define a timeout for trigger `Once`. If the timeout is reached before the micro-batch is processed, it won't be completed and no data will be committed. Such a behavior seems quite unpredictable and therefore we don't recommend it.

See the [Spark Documentation](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#triggers) for more information about triggers.

#### Hyperdrive Context
`HyperdriveContext` is an object intended to be used by the components to share data. It is a key-value store,
where the key is a string and the value can be of any type. The following context variables are currently used by the default implementation.

| Name | Type | Description |
| :--- | :--- | :--- |
| key.column.prefix | String | If `ConfluentAvroDecodingTransformer` is configured to consume keys, it prefixes the key columns with `key__` such that they can be distinguished in the dataframe. If `key__` happens to be a prefix of a value column, a random alphanumeric string is used instead. |
| key.column.names | Seq[String] | If `ConfluentAvroDecodingTransformer` is configured to consume keys, it contains the original column names (without prefix) in the key schema. |
 
#### Secrets Providers

##### AWS SecretsManager
| Property Name                                            | Required | Description                                                                                                                                                                     |
|:---------------------------------------------------------|:--------:|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `secretsprovider.config.providers.<provider-id>.class`   |   Yes    | The fully qualified class name of the secrets provider. <provider-id> is an arbitrary string. Multiple secrets providers can be configured by supplying multiple <provider-id>s |
| `secretsprovider.config.defaultprovider`                 |    No    | The <provider-id> of the secrets provider to be used by default                                                                                                                 |
| `secretsprovider.secrets.<secret-id>.options.secretname` |   Yes    | The Secret name of the secret in AWS Secrets Manager. <secret-id> is an arbitrary string. Multiple secrets can be configured by supplying multiple <secret-id>s                 |
| `secretsprovider.secrets.<secret-id>.options.provider`   |    No    | The <provider-id> of the secrets provider to be used for this specific secret.                                                                                                  |
| `secretsprovider.secrets.<secret-id>.options.readasmap`  |    No    | Set to true if the secret should be interpreted as a json map, set to false if the value should be read as is. Default: true                                                    |
| `secretsprovider.secrets.<secret-id>.options.key`        |    No    | If the secret should be read as a map, specify the key whose value should be extracted as the secret                                                                            |
| `secretsprovider.secrets.<secret-id>.options.encoding`   |    No    | Decodes the secret. Valid values: `base64`                                                                                                                                       |

The Secrets Provider will fill the configuration property `secretsprovider.secrets.<secret-id>.secretvalue` with the secret value. This configuration key will be
available for string interpolation to be used by other configuration properties.

**Example**
- `secretsprovider.config.providers.awssecretsmanager.class=za.co.absa.hyperdrive.driver.secrets.implementation.aws.AwsSecretsManagerSecretsProvider`
- `secretsprovider.secrets.truststorepassword.provider=awssecretsmanager`
- `secretsprovider.secrets.truststorepassword.options.secretname=<the-secret-name>`
- `secretsprovider.secrets.truststorepassword.options.key=<the-secret-key>`
- `secretsprovider.secrets.truststorepassword.options.encoding=base64`
- `reader.option.kafka.ssl.truststore.password=${secretsprovider.secrets.truststorepassword.secretvalue}`

#### Other
Hyperdrive uses [Apache Commons Configuration 2](https://github.com/apache/commons-configuration). This allows
properties to be referenced, e.g. like so
```
transformer.[avro.decoder].schema.registry.url=http://localhost:8081
writer.kafka.schema.registry.url=${transformer.[avro.decoder].schema.registry.url}
```

### Workflow Manager
Hyperdrive ingestions may be triggered using the Workflow Manager, which is developed in a separate repository: https://github.com/AbsaOSS/hyperdrive-trigger

A key feature of the Workflow Manager are triggers, which define when an ingestion should be executed and how it should be requested. The workflow manager supports cron-based triggers as well as triggers that listen to a notification topic.


## How to build
- Scala 2.12, Spark 2.4 (default)
```
mvn clean install
```
- Scala 2.12, Spark 3.0
```
mvn clean install -Pscala-2.12,spark-3
```
- Scala 2.11, Spark 2.4
```
mvn scala-cross-build:change-version -Pscala-2.11,spark-2
mvn clean install -Pscala-2.11,spark-2
mvn scala-cross-build:restore-version
```

### E2E tests with Docker
E2E tests require a running Docker instance on the executing machine and are not executed by default.
To execute them, build using the profile `all-tests`
```
mvn clean test -Pall-tests
```

### How to measure code coverage
```shell
./>mvn clean install -Pscala-2.ZY,spark-Z,code-coverage
```
If module contains measurable data the code coverage report will be generated on path:
```
{project-path}\hyperdrive\{module}\target\site\jacoco
```
