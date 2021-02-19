#!/bin/bash
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
mvn -B release:prepare -Pscala-2.12,spark-3 -DreleaseVersion=${RELEASE_VERSION}

# DEPLOYMENT
git checkout tags/v${RELEASE_VERSION}
echo "DEPLOY SCALA-2.11 ARTIFACTS"
mvn scala-cross-build:change-version -Pscala-2.11
mvn clean deploy -DskipTests -Pdeploy,scala-2.11
mvn scala-cross-build:restore-version
git clean -f

echo "DEPLOY SCALA-2.12 ARTIFACTS"
mvn clean deploy -DskipTests -Pdeploy,scala-2.12,spark-3
