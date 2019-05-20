package core.persistance

import core.models.EventTrigger
import core.models.tables.JDBCProfile.profile._
import scala.concurrent.{ExecutionContext, Future}

trait EventTriggersRepository extends Repository {
  def getNewActiveTriggers(idsToFilter: Seq[Long])(implicit ec: ExecutionContext): Future[Seq[EventTrigger]]
  def getInactiveTriggers(ids: Seq[Long])(implicit ec: ExecutionContext): Future[Seq[Long]]
}

class EventTriggersRepositoryImpl extends EventTriggersRepository {

  override def getNewActiveTriggers(idsToFilter: Seq[Long])(implicit ec: ExecutionContext): Future[Seq[EventTrigger]] = db.run {(
    for {
      eventTrigger <- eventTriggerTable if !(eventTrigger.id inSet idsToFilter)
      workflow <- workflowTable if workflow.id === eventTrigger.workflowId && workflow.isActive
    } yield {
      eventTrigger
    }).result
  }

  override def getInactiveTriggers(ids: Seq[Long])(implicit ec: ExecutionContext): Future[Seq[Long]] = db.run {(
    for {
      eventTrigger <- eventTriggerTable if eventTrigger.id inSet ids
      workflow <- workflowTable if workflow.id === eventTrigger.workflowId && !workflow.isActive
    } yield {
      eventTrigger.id
    }).result
  }

}
