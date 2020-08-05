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

package za.co.absa.hyperdrive.ingestor.implementation.reader.parquet

import java.util.concurrent.CountDownLatch

private[reader] class ParquetStreamReaderSpy(
  path: String,
  waitForFiles: Boolean,
  checkForFilesInterval: Long,
  extraConfs: Map[String, String]) extends ParquetStreamReader(path, waitForFiles, checkForFilesInterval, extraConfs) {
  private val waitingForFilesLatch = new CountDownLatch(1)
  def awaitWaitingForFiles(): Unit = waitingForFilesLatch.await()
  override def waitingForFilesHookForTesting(): Unit = waitingForFilesLatch.countDown()
}
