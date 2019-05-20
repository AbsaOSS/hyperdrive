package core.persistance

import slick.dbio.DBIO
import core.models.Event
import core.models.tables.JDBCProfile.profile._
import scala.concurrent.{ExecutionContext, Future}

trait EventRepository extends Repository {
  def getAllEvents()(implicit executionContext: ExecutionContext): Future[Seq[Event]]
  def insertEvents(events: Seq[Event])(implicit executionContext: ExecutionContext): DBIOAction[Unit, NoStream, Effect.Write]
  def getExistEvents(sensorEventIds: Seq[String])(implicit executionContext: ExecutionContext): Future[Seq[String]]
  def insertEvent(event: Event)(implicit executionContext: ExecutionContext): Future[Int]
}

class EventRepositoryImpl extends EventRepository {

  override def getAllEvents()(implicit ec: ExecutionContext): Future[Seq[Event]] = db.run(
    eventTable.result
  )

  override def insertEvents(events: Seq[Event])(implicit ec: ExecutionContext): DBIOAction[Unit, NoStream, Effect.Write] =
    DBIO.seq(eventTable ++= events)

  override def getExistEvents(sensorEventIds: Seq[String])(implicit ec: ExecutionContext): Future[Seq[String]] = db.run(
    eventTable.filter(e =>  e.sensorEventId.inSet(sensorEventIds)).map(_.sensorEventId).result
  )

  override def insertEvent(event: Event)(implicit executionContext: ExecutionContext): Future[Int] = db.run {
    eventTable += event
  }

}