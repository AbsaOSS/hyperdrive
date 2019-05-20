package core.scheduler.utilities

import java.util

import org.apache.kafka.common.serialization.Deserializer
import play.api.libs.json.{JsValue, Json}

class KafkaJsonDeserializer extends Deserializer[Either[String, JsValue]] {

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def deserialize(topic: String, data: Array[Byte]): Either[String, JsValue] =
    try {
      Right(Json.parse(data))
    } catch {
      case _: Exception => Left(new String(data))
    }

  override def close(): Unit = {}

}