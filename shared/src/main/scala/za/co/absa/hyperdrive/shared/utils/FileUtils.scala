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

package za.co.absa.hyperdrive.shared.utils

import org.apache.hadoop.fs.{FileSystem, Path}

private[hyperdrive] object FileUtils {

  def exists(file: String)(implicit fs: FileSystem): Boolean = {
    fs.exists(new Path(file))
  }

  def notExists(file: String)(implicit fs: FileSystem): Boolean = !exists(file)

  def isDirectory(file: String)(implicit fs: FileSystem): Boolean = {
    fs.isDirectory(new Path(file))
  }

  def isNotDirectory(file: String)(implicit fs: FileSystem): Boolean = !isDirectory(file)

  def isEmpty(directory: String)(implicit fs: FileSystem): Boolean = {
    val path = new Path(directory)
    fs.exists(path) && !fs.listFiles(path, true).hasNext
  }
}
