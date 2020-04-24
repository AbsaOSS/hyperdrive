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
import org.apache.spark.sql.streaming.Trigger
import za.co.absa.hyperdrive.ingestor.api.writer.StreamWriterCommonAttributes
import za.co.absa.hyperdrive.ingestor.api.writer.TriggerTypeEnum._

import scala.util.{Failure, Success, Try}

object StreamWriterUtil {

  def getTrigger(configuration: Configuration, timeUnit: TimeUnit = TimeUnit.MILLISECONDS): Trigger = {
    getTriggerAsTry(configuration, timeUnit) match {
      case Success(value) => value
      case Failure(exception) => throw exception
    }
  }

  def getTriggerAsTry(configuration: Configuration, timeUnit: TimeUnit = TimeUnit.MILLISECONDS): Try[Trigger] = {
    val triggerTypeKey = StreamWriterCommonAttributes.keyTriggerType

    val once = Once.toString.toUpperCase
    val processingTime = ProcessingTime.toString.toUpperCase
    ConfigUtils.getOrNone(triggerTypeKey, configuration)
      .map(triggerType => triggerType.toUpperCase match {
        case `once` => Try(Trigger.Once())
        case `processingTime` => getProcessingTime(configuration)
          .flatMap(time => Try(Trigger.ProcessingTime(time, timeUnit)))
        case _ => Failure(new IllegalArgumentException(s"Invalid value for $triggerTypeKey. " +
          s"Got $triggerType, but expected either $Once or $ProcessingTime"))})
      .getOrElse(Try(Trigger.Once()))
  }

  private def getProcessingTime(configuration: Configuration): Try[Long] = {
    val processingTimeKey = StreamWriterCommonAttributes.keyTriggerProcessingTime
    val processingTime = ConfigUtils.getOrNone(processingTimeKey, configuration).getOrElse("0")
    Try(processingTime.toLong).recoverWith {
      case e: Exception => Failure(new IllegalArgumentException(s"Invalid value for $processingTimeKey. " +
        s"Expected number, but got $processingTime", e))
    }
  }
}
