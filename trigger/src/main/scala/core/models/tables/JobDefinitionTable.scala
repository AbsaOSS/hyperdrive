package core.models.tables

import core.models.{JobDefinition, JobParameters, Workflow}
import core.models.enums.JobTypes.JobType
import slick.lifted.{ForeignKeyQuery, ProvenShape}
import core.models.tables.JdbcTypeMapper._
import core.models.tables.JDBCProfile.profile._

final class JobDefinitionTable(tag: Tag) extends Table[JobDefinition](tag, _tableName = "job_definition") {

  def workflowId: Rep[Long] = column[Long]("workflow_id")
  def name: Rep[String] = column[String]("name")
  def jobType: Rep[JobType] = column[JobType]("job_type")
  def variables: Rep[Map[String, String]] = column[Map[String, String]]("variables")
  def maps: Rep[Map[String, Set[String]]] = column[Map[String, Set[String]]]("maps")
  def id: Rep[Long] = column[Long]("id", O.PrimaryKey, O.AutoInc, O.SqlType("BIGSERIAL"))

  def * : ProvenShape[JobDefinition] = (workflowId, name, jobType, variables, maps, id) <> (
    jobDefinitionTuple =>
      JobDefinition.apply(
        workflowId = jobDefinitionTuple._1,
        name = jobDefinitionTuple._2,
        jobType = jobDefinitionTuple._3,
        jobParameters = JobParameters(
          variables = jobDefinitionTuple._4,
          maps = jobDefinitionTuple._5
        ),
        id = jobDefinitionTuple._6
      ),
    (jobDefinition: JobDefinition) =>
      Option(
        jobDefinition.workflowId,
        jobDefinition.name,
        jobDefinition.jobType,
        jobDefinition.jobParameters.variables,
        jobDefinition.jobParameters.maps,
        jobDefinition.id
      )
  )

  def workflow: ForeignKeyQuery[WorkflowTable, Workflow] =
    foreignKey("workflow_fk", workflowId, TableQuery[WorkflowTable])(_.id)

}