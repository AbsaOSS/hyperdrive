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

import org.apache.commons.configuration2.BaseConfiguration
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import za.co.absa.hyperdrive.ingestor.api.writer.StreamWriterCommonAttributes.{keyTriggerProcessingTime, keyTriggerType}

class TestStreamWriterUtil extends AnyFlatSpec with Matchers with MockitoSugar {

  behavior of s"StreamWriterUtil"

  "getTriggerAsTry" should "configure a one-time trigger by default" in {
    val configuration = new BaseConfiguration()

    val trigger = StreamWriterUtil.getTriggerAsTry(configuration).get

    trigger shouldBe Trigger.Once()
  }

  it should "configure a one-time trigger if configured" in {
    val configuration = new BaseConfiguration()
    configuration.setProperty(keyTriggerType, "Once")

    val trigger = StreamWriterUtil.getTriggerAsTry(configuration).get

    trigger shouldBe Trigger.Once()
  }

  it should "configure a processing trigger if configured with interval 0 ms by default" in {
    val configuration = new BaseConfiguration()
    configuration.setProperty(keyTriggerType, "ProcessingTime")

    val trigger = StreamWriterUtil.getTriggerAsTry(configuration).get

    trigger shouldBe Trigger.ProcessingTime(0L)
  }

  it should "configure a processing trigger if configured with configured interval" in {
    val configuration = new BaseConfiguration()
    configuration.setProperty(keyTriggerType, "ProcessingTime")
    configuration.setProperty(keyTriggerProcessingTime, "20000")

    val trigger = StreamWriterUtil.getTriggerAsTry(configuration).get

    trigger shouldBe Trigger.ProcessingTime(20000L)
  }

  it should "throw an exception if the trigger type configuration value is invalid" in {
    val configuration = new BaseConfiguration()
    configuration.setProperty(keyTriggerType, "invalid-trigger")

    val trigger = StreamWriterUtil.getTriggerAsTry(configuration)

    trigger.isFailure shouldBe true
    trigger.failed.get.getMessage should include(keyTriggerType)
  }

  it should "throw an exception if the processing time value is not a number" in {
    val configuration = new BaseConfiguration()
    configuration.setProperty(keyTriggerType, "ProcessingTime")
    configuration.setProperty(keyTriggerProcessingTime, "10s")

    val trigger = StreamWriterUtil.getTriggerAsTry(configuration)

    trigger.isFailure shouldBe true
    trigger.failed.get.getMessage should include(keyTriggerProcessingTime)
  }

  "getTrigger" should "return the trigger" in {
    val configuration = new BaseConfiguration()

    val trigger = StreamWriterUtil.getTrigger(configuration)

    trigger shouldBe Trigger.Once()
  }


  it should "throw an exception if it fails" in {
    val configuration = new BaseConfiguration()
    configuration.setProperty(keyTriggerType, "invalid-trigger")

    val exception = intercept[IllegalArgumentException](StreamWriterUtil.getTrigger(configuration))

    exception.getMessage should include(keyTriggerType)
  }
}
