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

package za.co.absa.hyperdrive.ingestor.api.utils

import java.util.concurrent.TimeUnit

import org.apache.commons.configuration2.Configuration
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}
import za.co.absa.hyperdrive.ingestor.api.writer.StreamWriterCommonAttributes

import scala.util.Try

object StreamWriterUtil {

  def configureTrigger(streamWriter: DataStreamWriter[Row], processingTimeOpt: Option[Long],
                       timeUnit: TimeUnit = TimeUnit.MILLISECONDS): DataStreamWriter[Row] = {
    processingTimeOpt match {
      case Some(processingTime) => streamWriter.trigger(Trigger.ProcessingTime(processingTime, timeUnit))
      case None => streamWriter.trigger(Trigger.Once())
    }
  }

  def getTriggerProcessingTime(configuration: Configuration,
                               processingTimeKey: String = StreamWriterCommonAttributes.keyTriggerProcessingTime): Option[Long] = {
    ConfigUtils.getOrNone(processingTimeKey, configuration)
      .flatMap(processingTimeValue => Try(processingTimeValue.toLong).toOption)
  }

}
