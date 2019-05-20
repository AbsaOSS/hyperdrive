package core.models

import play.api.libs.json.JsValue

case class Event(
  sensorEventId: String,
  payload: JsValue,
  id: Long = 0
)