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

package za.co.absa.hyperdrive.ingestor.implementation.utils

import java.time.Duration

import net.manub.embeddedkafka.{EmbeddedKafka, Consumers}
import org.apache.kafka.common.serialization.{Deserializer, Serializer, StringDeserializer, StringSerializer}
import org.scalatest.{Assertion, FlatSpec, Matchers}

class TestKafkaUtil extends FlatSpec with Matchers with EmbeddedKafka with Consumers {

  "getAllAvailableMessages" should "get all available messages" in {
//    implicit val kafkaConsumerTimeout: Duration = Duration.ofMillis(500L)
//
//    withRunningKafka {
//      implicit val serializer: Serializer[String] = new StringSerializer()
//      implicit val deserializer: Deserializer[String] = new StringDeserializer()
//      val messagesCount = 10000
//      (1 to messagesCount).foreach(i => EmbeddedKafka.publishToKafka("test-topic", s"message_${i}"))
//
//
//      withConsumer[String, String, Assertion](consumer => {
//        val messages = KafkaUtil.getAllAvailableMessages(consumer)
//        messages.size shouldBe messagesCount
//        val values = messages.map(c => c.value())
//        values should contain theSameElementsAs (1 to messagesCount).map(i => s"message_$i")
//      })
//    }
  }
}


