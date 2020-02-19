<!--
  ~
  ~ Copyright 2018 ABSA Group Limited
  ~
  ~  Licensed under the Apache License, Version 2.0 (the "License");
  ~  you may not use this file except in compliance with the License.
  ~  You may obtain a copy of the License at
  ~
  ~      http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~  Unless required by applicable law or agreed to in writing, software
  ~  distributed under the License is distributed on an "AS IS" BASIS,
  ~  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~  See the License for the specific language governing permissions and
  ~  limitations under the License.
  ~
  -->

# Hyperdrive Component Archetype

This is a Maven archetype for creating custom Hyperdrive components.

## Creating a custom Hyperdrive components project


Download the artifact to your local maven repository
```
mvn dependency:get -Dartifact=za.co.absa.hyperdrive:component-archetype:3.0.0 
```

Update the local archetype catalog
```
mvn archetype:crawl
```

Generate a skeleton project by executing the following command
```
mvn archetype:generate \
    -DarchetypeGroupId=za.co.absa.hyperdrive \
    -DarchetypeArtifactId=component-archetype \
    -DarchetypeVersion=3.0.0 \
    -DgroupId=<groupId> \
    -DartifactId=<artifactId> \
    -Dversion=<artifact-version> 
```
- `<groupId>` is your group id, e.g. com.acme,
- `<artifactId>` is the name for your artifact, e.g. mytransformer,
- `<artifact-version>` is the version number of the artifact, e.g. 0.1.0-SNAPSHOT

## Implementing a Hyperdrive component

There are five types of Hyperdrive components: Reader, Decoder, Transformer, Writer and Manager.

### Component stubs

This archetype provides stubs for all types, which are located under `<project-root>/src/main/scala/<groupId>/`

For example, if you need to implement a custom transformer, you should modify the stubs in
 `<project-root>/src/main/scala/{groupId}/transformer/mycomponent/MyStreamTransformerImpl`

If you don't need to implement a component type, you should delete the corresponding stubs. 
 E.g. if you don't need to implement the writer, delete the folder `<project-root>/src/main/scala/{groupId}/writer`

### Service provider configuration

Components need to be registered using the Java Service Provider Interface (SPI). There are configuration file templates
 under `<project-root>/src/resources/META-INF/services`. If you don't need a component type, 
 you should delete the corresponding configuration files.
 
A model test to verify the configuration is available under `<project-root>/test/scala/ServiceProviderConfigurationTest.scala`

## Building Hyperdrive components
```
% cd <target-directory>/<groupId>
% mvn clean package
```

creates the jar-file in the target, which contains the hyperdrive components.
