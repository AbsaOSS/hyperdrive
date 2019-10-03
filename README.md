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

# Hyperdrive - A Lambda Fast-to-Batch Ingestion Framework for Spark

### Build Status
| master | develop |
| ------------- | ------------- |
| [![Build Status](https://opensource.bigusdatus.com/jenkins/buildStatus/icon?job=Absa-OSS-Projects%2Fhyperdrive%2Fmaster)](https://opensource.bigusdatus.com/jenkins/job/Absa-OSS-Projects/job/hyperdrive/job/master/) | [![Build Status](https://opensource.bigusdatus.com/jenkins/buildStatus/icon?job=Absa-OSS-Projects%2Fhyperdrive%2Fdevelop)](https://opensource.bigusdatus.com/jenkins/job/Absa-OSS-Projects/job/hyperdrive/job/develop/) | 

## What is Hyperdrive?

Hyperdrive is a configurable and scalable ingestion platform that allows data movement and transformation from the fast to the batch layer in a Lambda Architecture on top of Apache Spark.

It is composed of two main modules: triggers and ingestors.

The triggers define when ingestions must happen, the ingestor defines how.

The trigger is developed in a separate repository: https://github.com/AbsaOSS/hyperdrive-trigger

## Motivation
In environments composed of multiple and diverse data sources, getting access to all of them as well as creating a data lake is quite a challenging task.

On top of that, the need for near real time has become a trend. In some cases, reacting to end-of-day files in batch way is simply not acceptable anymore. 

One approach that has been gathering momentum to unify those worlds is to use the so called Lambda Architecture (http://lambda-architecture.net/), by defining two layers, fast and batch, and how they should play together. However, there are two main issues. 

First, for large organizations, it is simply not possible to have all source systems to implement their own Lambdas, due to technical, organizational, priority or political issues - or all of those together.

Second, the ingestion from fast into batch layer is not yet standardized in the industry. Parameters such as periodicity might be source-dependent, which makes it harder to create a silver bullet solution that is resource-savvy.

From that, if the task of deciding when and what to ingest is put on data producers' shoulders, it becomes possible to create a general solution for ingestion. This is what Hyperdrive is intended to be.

## Architecture
Hyperdrive separates the framework in two independent components: ingestors and triggers. For the rest of this document, an ingestion will mean data movement from the fast into the batch layer, with or without transformations.

### Ingestors

Ingestors define how an ingestion must happen. To do that, it defines 6 components: readers, managers, decoders, transformers, writers and drivers.

#### Readers
Readers define how to connect to sources, e.g. how to connect to Kafka in a secure cluster by providing security directives, which topic and brokers to connect to.

#### Managers
Managers define extra configurations for readers and writers, e.g. Kafka offset settings.

#### Decoders
Decoders define how to convert the payload into DataFrames, e.g. decoding from binary into Avro after retrieving the schema from schema registry. 

#### Transformers
Defines transformations to be applied to the decoded DataFrame, e.g. dropping columns

#### Writers
Defines where DataFrames should be sent after the transformations, e.g. into HDFS as Parquet files.

#### Finalizers
Defines finalization / clean-up tasks after the ingestion has been finished.

#### Drivers
Configures the whole pipeline after receiving the configuration specification, e.g. a properties file.


### Triggers
Triggers define when an ingestion should be executed and how it should be requested.

To do this, they need need to define triggering policies, capacity management, Spark job watching capabilities and failure recovery features.

Triggering policies are configurable. For watching jobs and recovering from failures, triggers use Spark Launchers provided by Spark itself, databases to persist the state, and YARN REST apis to connect to the cluster after a failure. For capacity management, triggers can be configured to not allow more than a given number of parallel ingestions.


## Usage
To use Hyperdrive, it is first necessary to configure the trigger and the ingestor.

### Ingestor
To use Hyperdrive Ingestor, it is necessary to first configure it. The template located at ```driver/src/resources/Ingestion.properties.template``` exemplifies the configurations.

The settings starting with ```component.``` define which components should implement each of the responsibilities described in [Ingestors](#ingestors).

Settings starting with ```reader.``` will be used by [readers](#readers), the ones starting with ```manager.``` will configure [managers](#managers), and so on, for each component. 

For now, there is a Hyperdrive driver located at ```za.co.absa.hyperdrive.driver.drivers.PropertiesIngestionDriver```, in the ```driver``` module. Upon running ```mvn clean package``` at the root level of the project, a runnable JAR will be generated under ```target``` in the driver module. You can invoke that JAR via ```spark-submit```.

Due to Spark-Kafka integration issues, it will **only work with Spark 2.3+**.

The built-in components have the following capabilities:

- *reader* - Connects to a secure Kafka broker.
- *manager* - defines checkpoints for the stream reader and writer.
- *decoder* - decodes the payload as Confluent Avro, retrieving the schema from the specified Schema Registry.
- *transformer* - selects all columns from the decoded DataFrame.
- *writer* - writes the DataFrame as Parquet, in **append** mode, by invoking Spark's ```processAllAvailable``` method on the stream writer.
- *finalizer* - no operation is carried out.


### Trigger
*Coming soon.*

## Extensions
Both, Hyperdrive trigger and ingestor are extensible.

### Ingestor
Custom components can be implemented using the [Component Archetype](component-archetype) following the API defined in the package `za.co.absa.hyperdrive.ingestor.api`
- A custom component has to be a class which extends either of the abstract classes `StreamReader`, `OffsetManager`, `StreamDecoder`, `StreamTransformer`, `StreamWriter` or `IngestionFinalizer` 
- The class needs to have a companion object which implements the corresponding trait `StreamReaderFactory`, `OffsetManagerFactory`, `StreamDecoderFactory`, `StreamTransformerFactory`, `StreamWriterFactory` or `IngestionFinalizerFactory`
- The implemented components have to be packaged to a jar file, which can then be added to the classpath of the driver. To use a component, it has to be configured as described under [Usage](#usage)

After that, the new component will be able to be seamlessly invoked from the driver.

### Trigger
*Coming soon.*

## Features

Hyperdrive defines a notification topic that serves as a trigger for ingestions.

The structure of the messages in the notification topic takes into account the payload topic to be offloaded into HDFS, the starting offsets, the number of messages to be processed, the destination directory, etc. 

Hyperdrive supports Schema Registry connection and Confluent Avro messages (through [ABRiS](https://github.com/AbsaOSS/ABRiS)), which makes it capable of seamlessly handling whatever schemas the payload messages are using.

There is also a load balancer that controls the number of active Spark ingestor jobs at a given time.
