package core.api.rest.services

import core.models.{Workflow, WorkflowJoined}
import core.persistance.WorkflowRepository
import scala.concurrent.{ExecutionContext, Future}

trait WorkflowService {
  val workflowRepository: WorkflowRepository

  def createWorkflow(workflow: WorkflowJoined)(implicit ec: ExecutionContext): Future[Boolean]
  def getWorkflow(id: Long)(implicit ec: ExecutionContext): Future[Option[WorkflowJoined]]
  def getWorkflows()(implicit ec: ExecutionContext): Future[Seq[Workflow]]
  def deleteWorkflow(id: Long)(implicit ec: ExecutionContext): Future[Boolean]
  def updateWorkflow(workflow: WorkflowJoined)(implicit ec: ExecutionContext): Future[Boolean]
  def updateWorkflowActiveState(id: Long, isActive: Boolean)(implicit ec: ExecutionContext): Future[Boolean]
}

class WorkflowServiceImpl(override val workflowRepository: WorkflowRepository) extends WorkflowService {

  def createWorkflow(workflow: WorkflowJoined)(implicit ec: ExecutionContext): Future[Boolean] = {
    workflowRepository.insertWorkflow(workflow).map(_ => true)
  }

  def getWorkflow(id: Long)(implicit ec: ExecutionContext): Future[Option[WorkflowJoined]] = {
    workflowRepository.getWorkflow(id).map {
      case Some((w, e, jd)) =>
        Option(
          WorkflowJoined(
            id = w.id,
            name = w.name,
            isActive = w.isActive,
            created = w.created,
            updated = w.updated,
            trigger = e,
            job = jd
          )
        )
      case None => None
    }
  }

  def getWorkflows()(implicit ec: ExecutionContext): Future[Seq[Workflow]] = {
    workflowRepository.getWorkflows()
  }

  def deleteWorkflow(id: Long)(implicit ec: ExecutionContext): Future[Boolean] = {
    workflowRepository.deleteWorkflow(id).map(_ => true)
  }

  override def updateWorkflow(workflow: WorkflowJoined)(implicit ec: ExecutionContext): Future[Boolean] = {
    workflowRepository.updateWorkflow(workflow).map(_ => true)
  }

  override def updateWorkflowActiveState(id: Long, isActive: Boolean)(implicit ec: ExecutionContext): Future[Boolean] = {
    workflowRepository.updateWorkflowActiveState(id, isActive: Boolean).map(_ => true)
  }

}
