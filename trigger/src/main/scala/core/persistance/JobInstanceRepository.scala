package core.persistance

import java.time.LocalDateTime

import slick.dbio.{DBIOAction, Effect, NoStream}
import core.models.{JobInstance, OverallStatistics, PerWorkflowStatistics}
import slick.dbio.DBIO
import core.models.tables.JdbcTypeMapper._
import core.models.enums.JobStatuses
import core.models.enums.JobStatuses.{InQueue, Succeeded}
import core.models.tables.JDBCProfile.profile._
import scala.concurrent.{ExecutionContext, Future}

trait JobInstanceRepository extends Repository {
  def insertJobInstances(jobs: Seq[JobInstance], transactionDBIO: DBIOAction[Unit, NoStream, Effect.Write])(implicit ec: ExecutionContext): Future[Unit]
  def updateJob(job: JobInstance)(implicit ec: ExecutionContext): Future[Unit]
  def getNewActiveJobs(jobsIdToFilter: Seq[Long], size: Int)(implicit ec: ExecutionContext): Future[Seq[JobInstance]]
  def getJobInstances(jobDefinitionId: Long)(implicit ec: ExecutionContext): Future[Seq[JobInstance]]
  def getOverallStatistics()(implicit ec: ExecutionContext): Future[OverallStatistics]
  def getPerWorkflowStatistics()(implicit ec: ExecutionContext): Future[Seq[PerWorkflowStatistics]]
}

class JobInstanceRepositoryImpl extends JobInstanceRepository {

  override def insertJobInstances(
    jobs: Seq[JobInstance], transactionDBIO: DBIOAction[Unit, NoStream, Effect.Write]
  )(implicit ec: ExecutionContext): Future[Unit] = db.run {
    transactionDBIO andThen DBIO.seq(jobInstanceTable ++= jobs)
  }

  override def updateJob(job: JobInstance)(implicit ec: ExecutionContext): Future[Unit] = db.run {
    jobInstanceTable.filter(_.id === job.id).update(job.copy(updated = Option(LocalDateTime.now()))).andThen(DBIO.successful((): Unit))
  }

  override def getNewActiveJobs(idsToFilter: Seq[Long], size: Int)(implicit ec: ExecutionContext): Future[Seq[JobInstance]] = db.run(
    jobInstanceTable.filter(
      ji =>
        !ji.id.inSet(idsToFilter) && ji.jobStatus.inSet(JobStatuses.finalStatuses)
    ).take(size).result
  )

  override def getJobInstances(jobDefinitionId: Long)(implicit ec: ExecutionContext): Future[Seq[JobInstance]] = db.run(
    jobInstanceTable.filter(_.jobDefinitionId === jobDefinitionId).sortBy(_.id).result
  )

  override def getOverallStatistics()(implicit ec: ExecutionContext): Future[OverallStatistics] = db.run {
    (
      jobInstanceTable.filter(_.jobStatus.inSet(Seq(Succeeded))).size,
      jobInstanceTable.filter(_.jobStatus.inSet(JobStatuses.statuses.filter(_.isFailed))).size,
      jobInstanceTable.filter(_.jobStatus.inSet(JobStatuses.statuses.filter(_.isRunning))).size,
      jobInstanceTable.filter(_.jobStatus.inSet(Seq(InQueue))).size
    ).result.map((OverallStatistics.apply _).tupled(_))
  }

  override def getPerWorkflowStatistics()(implicit ec: ExecutionContext): Future[Seq[PerWorkflowStatistics]] = db.run {(
    for {
      workflow <- workflowTable
      jobDefinition <- jobDefinitionTable.filter(_.workflowId === workflow.id)
    } yield {
      val jobInstances = jobInstanceTable.filter(_.jobDefinitionId === jobDefinition.id)
      (
        jobDefinition.id,
        workflow.name,
        workflow.isActive,
        jobInstances.size,
        jobInstances.filter(_.jobStatus.inSet(Seq(Succeeded))).size,
        jobInstances.filter(_.jobStatus.inSet(JobStatuses.statuses.filter(_.isFailed))).size,
        jobInstances.filter(_.jobStatus.inSet(Seq(InQueue))).size,
        jobInstances.filter(_.jobStatus.inSet(JobStatuses.statuses.filter(_.isRunning))).size
      )
    }).sortBy(_._2).result
  }.map(_.map((PerWorkflowStatistics.apply _).tupled(_)))

}