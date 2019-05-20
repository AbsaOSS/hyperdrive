package core.models.tables

import core.models.tables.JDBCProfile.profile._
import core.models.enums.EventTypes.EventType
import core.models.{EventTrigger, Properties, TriggerProperties, Workflow}
import slick.lifted.{ForeignKeyQuery, ProvenShape}
import core.models.tables.JdbcTypeMapper._

final class EventTriggerTable(tag: Tag) extends Table[EventTrigger](tag, _tableName = "event_trigger") {

  def workflowId: Rep[Long] = column[Long]("workflow_id")
  def eventType: Rep[EventType] = column[EventType]("event_type")
  def variables: Rep[Map[String, String]] = column[Map[String, String]]("variables")
  def maps: Rep[Map[String, Set[String]]] = column[Map[String, Set[String]]]("maps")
  def matchProperties: Rep[Map[String, String]] = column[Map[String, String]]("match_properties")
  def id: Rep[Long] = column[Long]("id", O.PrimaryKey, O.AutoInc, O.SqlType("BIGSERIAL"))

  def * : ProvenShape[EventTrigger] = (workflowId, eventType, variables, maps, matchProperties, id) <> (
    eventTriggerTuple =>
      EventTrigger.apply(
        workflowId = eventTriggerTuple._1,
        eventType = eventTriggerTuple._2,
        triggerProperties = TriggerProperties.apply(
          eventTriggerId = eventTriggerTuple._6,
          properties = Properties.apply(
            variables = eventTriggerTuple._3,
            maps = eventTriggerTuple._4
          ),
          matchProperties = eventTriggerTuple._5
        ),
        id = eventTriggerTuple._6
      ),
    (event: EventTrigger) =>
      Option(
        event.workflowId,
        event.eventType,
        event.triggerProperties.properties.variables,
        event.triggerProperties.properties.maps,
        event.triggerProperties.matchProperties,
        event.id
      )
  )

  def workflow: ForeignKeyQuery[WorkflowTable, Workflow] =
    foreignKey("workflow_fk", workflowId, TableQuery[WorkflowTable])(_.id)

}