<!--
  ~
  ~ Copyright 2019 ABSA Group Limited
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

Hyperdrive Component Archetype
==============================

This is a Maven archetype for creating custom Hyperdrive components.

Creating a custom Hyperdrive components project
-----------------------------------------------

You can generate a skeleton project by executing the following command:

```
% mvn archetype:generate \
    -DarchetypeGroupId=za.co.absa.hyperdrive \
    -DarchetypeArtifactId=component-archetype \
    -DarchetypeVersion=<hyperdrive-version> \
    -DgroupId=<groupId> \
    -DartifactId=<artifactId> \
    -Dversion=<component-version> \
    -DoutputDirectory=<target-directory>
```
- `<hyperdrive-version>` is the version of this archetype, (currently 1.0.0-SNAPSHOT)
- `<groupId>` is your group id, e.g. com.acme,
- `<artifactId>` is the name for your artifact, e.g. mytransformer,
- `<artifact-version>` is the version number of the artifact, e.g. 1.0-SNAPSHOT and
- `<target-directory>` should point to the directory where the project for the custom component should be created, e.g. /tmp/mytransformer

Implementing a Hyperdrive component
-----------------------------------

There are five types of Hyperdrive components: Decoder, Manager, Reader, Transformer and Writer.

This archetype provides stubs for all types, which are located under `<project-root>/src/main/scala/<groupId>/`

For example, if you need to implement a custom transformer, you should modify the stubs `<project-root>/src/main/scala/{groupId}/transformer/mycomponent/MyStreamTransformerImpl`

If you don't need to implement a component type, you should delete the corresponding stubs. E.g. if you don't need to implement the writer, delete the folder `<project-root>/src/main/scala/{groupId}/writer`


Building Hyperdrive components
------------------------------
```
% cd <target-directory>/<groupId>
% mvn clean package
```

creates the jar-file in the target, which contains the hyperdrive components.
