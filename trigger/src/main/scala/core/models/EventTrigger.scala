package core.models

import core.models.enums.EventTypes.EventType

case class EventTrigger(
  workflowId: Long,
  eventType: EventType,
  triggerProperties: TriggerProperties,
  id: Long = 0
)

case class TriggerProperties(
  eventTriggerId: Long,
  properties: Properties,
  matchProperties: Map[String, String]
)

case class Properties(
  variables: Map[String, String],
  maps: Map[String, Set[String]]
)