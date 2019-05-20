package core.api.rest.services

import core.models.{JobInstance, OverallStatistics, PerWorkflowStatistics}
import core.persistance.JobInstanceRepository
import scala.concurrent.{ExecutionContext, Future}

trait JobInstanceService {
  val jobInstanceRepository: JobInstanceRepository
  def getJobInstances(jobDefinitionId: Long)(implicit ec: ExecutionContext): Future[Seq[JobInstance]]
  def getOverallStatistics()(implicit ec: ExecutionContext): Future[OverallStatistics]
  def getPerWorkflowStatistics()(implicit ec: ExecutionContext): Future[Seq[PerWorkflowStatistics]]
}

class JobInstanceServiceImpl(override val jobInstanceRepository: JobInstanceRepository) extends JobInstanceService {

  override def getJobInstances(jobDefinitionId: Long)(implicit ec: ExecutionContext): Future[Seq[JobInstance]] = {
    jobInstanceRepository.getJobInstances(jobDefinitionId)
  }

  override def getOverallStatistics()(implicit ec: ExecutionContext): Future[OverallStatistics] = {
    jobInstanceRepository.getOverallStatistics()
  }

  override def getPerWorkflowStatistics()(implicit ec: ExecutionContext): Future[Seq[PerWorkflowStatistics]] = {
    jobInstanceRepository.getPerWorkflowStatistics()
  }

}
