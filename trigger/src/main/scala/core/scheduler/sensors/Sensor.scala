package core.scheduler.sensors

import core.models.{Event, TriggerProperties}

import scala.concurrent.{ExecutionContext, Future}

trait Sensor {
  val eventsProcessor: (Seq[Event], TriggerProperties) => Future[Boolean]
  val triggerProperties: TriggerProperties
  implicit val executionContext: ExecutionContext
  def close(): Unit
}

abstract class PollSensor(
  override val eventsProcessor: (Seq[Event], TriggerProperties) => Future[Boolean],
  override val triggerProperties: TriggerProperties,
  override val executionContext: ExecutionContext
) extends Sensor {
  implicit val ec: ExecutionContext = executionContext
  def poll(): Future[Unit]
}

abstract class PushSensor(
  override val eventsProcessor: (Seq[Event], TriggerProperties) => Future[Boolean],
  override val triggerProperties: TriggerProperties,
  override val executionContext: ExecutionContext
) extends Sensor {
  implicit val ec: ExecutionContext = executionContext
  def push(events: Seq[Event]): Future[Unit]
}