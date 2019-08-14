package core.models.tables

import play.api.libs.json.JsValue
import core.models.tables.JDBCProfile.profile._
import core.models.Event
import core.models.tables.JdbcTypeMapper._
import slick.lifted.ProvenShape

final class EventTable(tag: Tag) extends Table[Event](tag, _tableName = "event") {

  def sensorEventId: Rep[String] = column[String]("sensor_event_id", O.Length(70), O.Unique)
  def payload: Rep[JsValue] = column[JsValue]("payload")
  def id: Rep[Long] = column[Long]("id", O.PrimaryKey, O.AutoInc, O.SqlType("BIGSERIAL"))

  def * : ProvenShape[Event] = (sensorEventId, payload, id) <> (
    eventTuple =>
      Event.apply(
        sensorEventId = eventTuple._1,
        payload = eventTuple._2,
        id = eventTuple._3
      ),
      Event.unapply
  )

}
