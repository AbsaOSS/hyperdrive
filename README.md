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

# Hyperdrive - Kafka to HDFS Ingestion tool

Hyperdrive is a flexible ingestor platform that allows configurable ingestion from Kafka into HDFS. 

It is composed of Spark ingestion jobs and watchers for notification topics containing the parameters of the ingestion.

The notification topic serves as the "API" for the platform.


## Motivation

In environments composed of multiple and diverse data sources, getting access to all of them as well as creating a data lake is quite a challenging task.

One approach that has been gathering momentum is to use the so called Lambda Architecture (http://lambda-architecture.net/). However, the ingestion from Kafka into HDFS is not yet standardized in the industry. Parameters such as periodicity might be source-dependent which makes it harder to create a silver bullet solution.

On the other hand, if the task of deciding when and what to ingest is put on data producers' shoulders, it becomes possible to create a general solution for ingestion. This is what Hyperdrive is intended to be.

## Features

Hyperdrive defines a notification topic that serves as a trigger for ingestions.

The structure of the messages in the notification topic takes into account the payload topic to be offloaded into HDFS, the starting offsets, the number of messages to be processed, the destination directory, etc. 

Hyperdrive supports Schema Registry connection and Confluent Avro messages (through [ABRiS](https://github.com/AbsaOSS/ABRiS)), which makes it capable of seamlessly handling whatever schemas the payload messages are using.

There is also a load balancer that controls the number of active Spark ingestor jobs at a given time.
