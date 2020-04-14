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

import org.apache.commons.configuration2.{BaseConfiguration, Configuration}
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}
import org.mockito.ArgumentMatchers.{eq => eqTo}
import za.co.absa.hyperdrive.ingestor.api.writer.StreamWriterCommonAttributes

class TestStreamWriterUtil extends FlatSpec with Matchers with MockitoSugar {

  behavior of s"StreamWriterUtil"

  "configureTrigger" should "configure one-time trigger if processing-time is not defined" in {
    val dataStreamWriterMock = mock[DataStreamWriter[Row]]

    StreamWriterUtil.configureTrigger(dataStreamWriterMock, None)

    verify(dataStreamWriterMock).trigger(eqTo(Trigger.Once()))
  }

  it should "configure processing-time trigger if processing-time is defined" in {
    val dataStreamWriterMock = mock[DataStreamWriter[Row]]
    val interval = 2000L

    StreamWriterUtil.configureTrigger(dataStreamWriterMock, Some(interval))

    verify(dataStreamWriterMock).trigger(eqTo(Trigger.ProcessingTime(interval, TimeUnit.MILLISECONDS)))
  }

  "getTriggerProcessingTime" should "convert the value to Long if possible" in {
    val configPositiveCase = createConfiguration("60000")
    val configNegativeCaseNotANumber = createConfiguration("1 minute")
    val configNegativeCaseNotDefined = new BaseConfiguration

    val valuePositiveCase = StreamWriterUtil.getTriggerProcessingTime(configPositiveCase)
    val valueNegativeCaseNotANumber = StreamWriterUtil.getTriggerProcessingTime(configNegativeCaseNotANumber)
    val valueNegativeCaseNotDefined = StreamWriterUtil.getTriggerProcessingTime(configNegativeCaseNotDefined)

    valuePositiveCase shouldBe Some(60000L)
    valueNegativeCaseNotANumber shouldBe None
    valueNegativeCaseNotDefined shouldBe None
  }

  private def createConfiguration(triggerProcessingTime: String): Configuration = {
    val configuration = new BaseConfiguration()
    configuration.setProperty(StreamWriterCommonAttributes.keyTriggerProcessingTime, triggerProcessingTime)
    configuration
  }
}
