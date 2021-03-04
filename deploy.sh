#!/bin/bash
#
# Copyright 2018 ABSA Group Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e

# CHECK INPUTS
if [[ -z $1 ]]; then
  echo "Usage: ./deploy.sh <release-version>"
  echo "Example: ./deploy.sh 4.2.1"
  exit
fi
RELEASE_VERSION=$1

if [ -n "$(git status --porcelain)" ]; then
  echo "There are uncommitted changes. Please commit or discard them."
  exit
fi

# RELEASE
RELEASE_BRANCH="release/v${RELEASE_VERSION}"
git checkout -b ${RELEASE_BRANCH}
mvn -B release:prepare -Pscala-2.12,spark-2,spark-3 -DreleaseVersion=${RELEASE_VERSION}

# DEPLOYMENT
git checkout tags/v${RELEASE_VERSION}
echo "DEPLOY SPARK-2 / SCALA-2.11 ARTIFACTS"
mvn scala-cross-build:change-version -Pscala-2.11,spark-2
mvn clean deploy -DskipTests -Pdeploy,scala-2.11,spark-2
mvn scala-cross-build:restore-version
git clean -f

echo "DEPLOY SPARK-2 / SCALA-2.12 ARTIFACTS"
mvn clean deploy -DskipTests -Pdeploy,scala-2.12,spark-2

echo "DEPLOY SPARK-3 ARTIFACTS"
cd compatibility_spark-3
mvn clean deploy -DskipTests -Pdeploy,scala-2.12,spark-3
cd ..
cd hyperdrive-release_spark-3
mvn clean deploy -DskipTests -Pdeploy,scala-2.12,spark-3
