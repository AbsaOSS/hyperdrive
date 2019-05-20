package core.models

import java.time.LocalDateTime

case class WorkflowJoined(
  name: String,
  isActive: Boolean,
  created: LocalDateTime,
  updated: Option[LocalDateTime],
  trigger: EventTrigger,
  job: JobDefinition,
  id: Long = 0
){
  def toWorkflow: Workflow = {
    Workflow(
      name = this.name,
      isActive = this.isActive,
      created = this.created,
      updated = this.updated,
      id = this.id
    )
  }
}

case class Workflow(
  name: String,
  isActive: Boolean,
  created: LocalDateTime,
  updated: Option[LocalDateTime],
  id: Long
)

case class WorkflowState(
  isActive: Boolean
)