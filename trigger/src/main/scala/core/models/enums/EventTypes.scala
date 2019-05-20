package core.models.enums

object EventTypes {

  sealed abstract class EventType(val name: String) {
    override def toString: String = name
  }

  case object Kafka extends EventType("Kafka")

  val eventTypes: Set[EventType] = Set(Kafka)

}