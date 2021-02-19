#!/bin/bash
set -e

# CHECK INPUTS
if [[ -z $1 ]] || [[ -z $2 ]]; then
  echo "Usage: ./deploy.sh <release-version> <next-snapshot-version>"
  echo "Example: ./deploy.sh 4.2.1 4.2.2-SNAPSHOT"
  exit
fi
RELEASE_VERSION=$1
SNAPSHOT_VERSION=$2

RELEASE_REGEX="[0-9]+\.[0-9]+\.[0-9]+"
SNAPSHOT_REGEX="${RELEASE_REGEX}-SNAPSHOT"
if ! [[ ${RELEASE_VERSION} =~ ${RELEASE_REGEX} ]]; then
  read -p "${RELEASE_VERSION} doesn't look like a proper release version. Press Ctrl+C to abort, or any key to continue"
fi

if ! [[ ${SNAPSHOT_VERSION} =~ ${SNAPSHOT_REGEX} ]]; then
  read -p "${SNAPSHOT_VERSION} doesn't look like a proper snapshot version. Press Ctrl+C to abort, or any key to continue"
fi

if [ -n "$(git status --porcelain)" ]; then
  echo "There are uncommitted changes. Please commit or discard them."
  exit
fi

# RELEASE
RELEASE_BRANCH="release/v${RELEASE_VERSION}"
git checkout -b ${RELEASE_BRANCH}
mvn versions:set -DnewVersion=${RELEASE_VERSION} -DprocessAllModules=true -DgenerateBackupPoms=false
git add .
git commit -m "[release-script] Prepare release v${RELEASE_VERSION}"
mvn clean test
RELEASE_TAG="v${RELEASE_VERSION}"
git tag -a ${RELEASE_TAG} -m "[release-script] copy for tag ${RELEASE_TAG}"
mvn versions:set -DnewVersion=${SNAPSHOT_VERSION} -DprocessAllModules=true -DgenerateBackupPoms=false
git add .
git commit -m "[release-script] Prepare for next development iteration"
git push -u origin ${RELEASE_BRANCH}
git push origin ${RELEASE_TAG}

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
mvn deploy -DskipTests -Pdeploy,scala-2.12,spark-3
cd ..
cd hyperdrive-release_spark-3
mvn deploy -DskipTests -Pdeploy,scala-2.12,spark-3
