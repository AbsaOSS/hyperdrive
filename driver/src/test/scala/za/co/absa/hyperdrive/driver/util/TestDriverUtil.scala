/*
 * Copyright 2018 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.hyperdrive.driver.util


import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.hyperdrive.driver.utils.DriverUtil

class TestDriverUtil extends AnyFlatSpec with Matchers {
  behavior of "DriverUtil"

  it should "print the implementation version" in {
    // when
    val versionString = DriverUtil.getVersionString

    // then
    versionString should not be "Version: ${project.version}, Implementation-Version: ${buildNumnber}"
    versionString should fullyMatch regex "Version: .*, Implementation-Version: [a-zA-ZZ0-9]{8}"
  }
}
