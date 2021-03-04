#!/usr/bin/env bash
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

#
# This script creates the jar-file TestClassLoaderUtilsOther.jar which is required by the test class TestClassLoaderUtils
# The jar-file will consist only of the scala class TestClassLoaderUtilsSingletonInstanceOther (see below).
# It depends on the trait TestClassLoaderUtilsTestTrait. Any changes to that script might have to be reflected in this script.
#
# Caution: This script will execute `mvn clean test` in the module ingestor/shared
#

# Fail at first error
set -e

# Find root directory
cd "$(dirname "${BASH_SOURCE[0]}")"
cd ../../../../
ROOT_DIR="$(pwd)"

if [[ ${ROOT_DIR} != *"hyperdrive" ]]; then
  echo "ROOT_DIR is expected to be a directory with name 'hyperdrive', but is ${ROOT_DIR}"
  exit 1
fi

# Set directory and file names
MODULE_ROOT_DIR="${ROOT_DIR}/shared/"
PACKAGE="za/co/absa/hyperdrive/shared/utils/"
SRC_TEST_DIR="${MODULE_ROOT_DIR}/src/test/scala/"
RESOURCE_TEST_DIR="${MODULE_ROOT_DIR}/src/test/resources/"
TARGET_TEST_DIR="${MODULE_ROOT_DIR}/target/test-classes/"

SCALA_CLASSNAME="TestClassLoaderUtilsSingletonInstanceOther"
SCALA_SRC_FILE_REL="${PACKAGE}/${SCALA_CLASSNAME}.scala"
SCALA_SRC_FILE_ABS="${SRC_TEST_DIR}/${SCALA_SRC_FILE_REL}"
SCALA_CLASS_FILES_REL=("${PACKAGE}/${SCALA_CLASSNAME}\$.class" "${PACKAGE}/${SCALA_CLASSNAME}.class")

OUTPUT_JAR="${RESOURCE_TEST_DIR}/TestClassLoaderUtilsOther.jar"

# Create test file in test directory
touch "${SCALA_SRC_FILE_ABS}"
cat > "${SCALA_SRC_FILE_ABS}" <<-END
/*
 *  Copyright 2018 ABSA Group Limited
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package za.co.absa.hyperdrive.shared.utils

import za.co.absa.hyperdrive.shared.utils.classloaderutils.TestClassLoaderUtilsTestTrait

object TestClassLoaderUtilsSingletonInstanceOther extends TestClassLoaderUtilsTestTrait {
}

END

# Build module with test file
cd "${MODULE_ROOT_DIR}"
mvn clean test

# Package compiled classes into jar and clean up
cd "${TARGET_TEST_DIR}"
jar -cvf "${OUTPUT_JAR}" "${SCALA_CLASS_FILES_REL[@]}"
rm "${SCALA_CLASS_FILES_REL[@]}"

# Package source file and clean up
cd "${SRC_TEST_DIR}"
jar -uvf "${OUTPUT_JAR}" ${SCALA_SRC_FILE_REL}
rm "${SCALA_SRC_FILE_REL}"

echo "Jar-file has been successfully created at ${OUTPUT_JAR}"
