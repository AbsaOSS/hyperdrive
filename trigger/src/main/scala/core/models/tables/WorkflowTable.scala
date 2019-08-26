package core.models.tables

import java.time.LocalDateTime

import core.models.Workflow
import slick.lifted.ProvenShape
import core.models.tables.JDBCProfile.profile._

final class WorkflowTable(tag: Tag) extends Table[Workflow](tag, _tableName = "workflow") {
  def name: Rep[String] = column[String]("name", O.Unique, O.Length(45))
  def isActive: Rep[Boolean] = column[Boolean]("is_active")
  def created: Rep[LocalDateTime] = column[LocalDateTime]("created")
  def updated: Rep[Option[LocalDateTime]] = column[Option[LocalDateTime]]("updated")
  def id: Rep[Long] = column[Long]("id", O.PrimaryKey, O.AutoInc, O.SqlType("BIGSERIAL"))

  def * : ProvenShape[Workflow] = (name, isActive, created, updated, id) <> (
    workflowTuple =>
      Workflow.apply(
        name = workflowTuple._1,
        isActive = workflowTuple._2,
        created = workflowTuple._3,
        updated = workflowTuple._4,
        id = workflowTuple._5
      ),
    Workflow.unapply
  )

}