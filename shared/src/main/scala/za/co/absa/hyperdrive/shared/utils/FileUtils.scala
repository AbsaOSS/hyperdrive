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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, RemoteIterator}

import scala.collection.AbstractIterator

private[hyperdrive] object FileUtils {

  def exists(file: String, configuration: Configuration): Boolean = {
    val fileSystem = getFileSystem(configuration)
    fileSystem.exists(new Path(file))
  }

  def notExists(file: String, configuration: Configuration): Boolean = !exists(file, configuration)

  def isDirectory(file: String, configuration: Configuration): Boolean = {
    val fileSystem = getFileSystem(configuration)
    fileSystem.isDirectory(new Path(file))
  }

  def isNotDirectory(file: String, configuration: Configuration): Boolean = !isDirectory(file, configuration)

  def isEmpty(directory: String, configuration: Configuration): Boolean = {
    val fs = getFileSystem(configuration)
    val path = new Path(directory)
    fs.exists(path) && !fs.listLocatedStatus(path).hasNext
  }

  def dirContainsNoParquetFilesOrDoesNotExist(directory: String, configuration: Configuration): Boolean = {
    val fs = getFileSystem(configuration)
    val dirPath = new Path(directory)
    if (!fs.exists(dirPath)) {
      true
    } else {
      !containsParquetFile(fs, dirPath)
    }
  }

  private def containsParquetFile(fs: FileSystem, directory: Path): Boolean = {
    val currentDirContainsParquetFile = fs.listFiles(directory, false)
      .exists(f => f.isFile && f.getPath.getName.endsWith(".parquet"))
    if(currentDirContainsParquetFile) {
      true
    } else {
      fs.listLocatedStatus(directory)
        .filter(s => s.isDirectory)
        .filter(s => {
          val name = s.getPath.getName
          !name.startsWith("_") && !name.startsWith(".")
        })
        .exists(s => containsParquetFile(fs, s.getPath))
    }
  }

  private def getFileSystem(configuration: Configuration): FileSystem = {
    FileSystem.get(configuration)
  }

  implicit def remoteIterator2AbstractIterator[T](remoteIterator: RemoteIterator[T]): AbstractIterator[T] = {
    new AbstractIterator[T] {
      override def hasNext: Boolean = remoteIterator.hasNext
      override def next: T = remoteIterator.next
    }
  }
}
