/*
 *
 * Copyright 2019 ABSA Group Limited
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package test

import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import org.springframework.kafka.test.EmbeddedKafkaBroker

class ATest extends FlatSpec with BeforeAndAfterAll {

  var embeddedKafka = new EmbeddedKafkaBroker(1, true, "topic")

  override def beforeAll(): Unit = {
    println("Starting Kafka and Zookeeper test brokers")
    embeddedKafka.afterPropertiesSet()
  }

  override def afterAll(): Unit = {
    println("Shutting down Kafka and Zookeeper test brokers")
    embeddedKafka.destroy()
  }

  behavior of "x"

  it should "do something" in {
    println(s"Kafka test brokers = ${embeddedKafka.getBrokersAsString}")
    println(s"Zookeeper test brokers = ${embeddedKafka.getZookeeperConnectionString}")
  }
}
