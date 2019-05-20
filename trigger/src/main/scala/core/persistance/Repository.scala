package core.persistance

import core.models.tables._
import core.models.tables.JDBCProfile.profile._

trait Repository {
  val db = Database.forConfig("db")

  val eventTable = TableQuery[EventTable]
  val eventTriggerTable = TableQuery[EventTriggerTable]
  val jobDefinitionTable = TableQuery[JobDefinitionTable]
  val jobInstanceTable = TableQuery[JobInstanceTable]
  val workflowTable = TableQuery[WorkflowTable]
}