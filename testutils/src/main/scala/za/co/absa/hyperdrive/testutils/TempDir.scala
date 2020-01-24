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

package za.co.absa.hyperdrive.testutils

import java.io.{File, IOException}

object TempDir {

  private val tmpDir = if (System.getProperty("java.io.tmpdir") != null) System.getProperty("java.io.tmpdir") else ""

  /**
    * The parent directory for the temporary dir will be either, the directory defined in system properties under key "java.io.tmpdir"
    * or the local directory where this code is executed.
    *
    * The temporary directory name will be "currentTimeMillis-test-dir" (e.g. 111113273883-test-dir)
    *
    * The returned file already exists and is marked for deletion on exit.
    */
  @throws[IOException]
  def getNew: File = {
    val tempDir = new File(new File(tmpDir), System.currentTimeMillis() + "-test-dir/"); // tmpDir/8377740/
    if (!tempDir.exists() && !tempDir.mkdirs()) {
      throw new IOException(s"Could not create directory at ${tempDir.getAbsolutePath}.")
    }
    tempDir.deleteOnExit()
    tempDir
  }
}
