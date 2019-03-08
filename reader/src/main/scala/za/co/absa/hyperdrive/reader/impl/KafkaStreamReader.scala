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

package za.co.absa.hyperdrive.reader.impl

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.DataStreamReader
import za.co.absa.hyperdrive.reader.StreamReader
import za.co.absa.hyperdrive.shared.InfrastructureSettings.KafkaSettings

class KafkaStreamReader(topic: String, brokers: String) extends StreamReader {

  override def read(spark: SparkSession): DataStreamReader = {
    spark
      .readStream
      .format(KafkaSettings.STREAM_FORMAT_KAFKA_NAME)
      .option(KafkaSettings.TOPIC_SUBSCRIPTION_KEY, topic)
      .option(KafkaSettings.SPARK_BROKERS_SETTING_KEY, brokers)
  }
}
