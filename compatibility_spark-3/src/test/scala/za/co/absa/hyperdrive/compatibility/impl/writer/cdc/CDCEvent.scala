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

package za.co.absa.hyperdrive.compatibility.impl.writer.cdc

import java.sql.Timestamp

case class CDCEvent(id: String, value: String, timestamp: Timestamp, eventType: String)

object CDCEvent {
  def loadFromFile(path: String): Seq[CDCEvent] = {
    val lines = FileUtils.readFileLines(getClass.getResource(path).getPath)
    for {
      line <- lines
      values = line.split(",").map(_.trim)
    } yield CDCEvent(values(0), values(1), Timestamp.valueOf(values(2)), values(3))
  }
}
