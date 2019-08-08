package core.scheduler.sensors

import java.util.concurrent.Executors

import core.models.enums.EventTypes
import core.persistance.EventTriggersRepository
import core.scheduler.sensors.kafka.KafkaSensor
import core.scheduler.utilities.SensorsConfig
import core.scheduler.eventProcessor.EventProcessor
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

class Sensors(eventProcessor: EventProcessor, eventTriggersRepository: EventTriggersRepository) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private implicit val executionContext: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(Executors.newFixedThreadPool(SensorsConfig.getThreadPoolSize))

  private val sensors: mutable.Map[Long, Sensor] = mutable.Map.empty[Long, Sensor]

  def processEvents(): Future[Unit] = {
    logger.debug(s"Processing events. Sensors: ${sensors.keys}")
    val fut = for {
      _ <- removeInactiveSensors()
      _ <- addNewSensors()
      _ <- pollEvents()
    } yield {
      (): Unit
    }

    fut.onComplete {
      case Success(_) => logger.debug("Processing events successful")
      case Failure(exception) => {
        logger.debug("Processing events failed.", exception)
      }
    }

    fut
  }

  private def removeInactiveSensors(): Future[Unit] = {
    val activeSensors = sensors.keys.toSeq
    eventTriggersRepository.getInactiveTriggers(activeSensors).map(
      _.foreach{
        id =>
          sensors.get(id).foreach(_.close())
          sensors.remove(id)
      }
    )
  }

  private def addNewSensors(): Future[Unit] = {
    val activeSensors = sensors.keys.toSeq
    eventTriggersRepository.getNewActiveTriggers(activeSensors).map {
      _.foreach {
        case eventTrigger if eventTrigger.eventType == EventTypes.Kafka || eventTrigger.eventType == EventTypes.AbsaKafka =>
          sensors.put(
            eventTrigger.id, new KafkaSensor(eventProcessor.eventProcessor, eventTrigger.triggerProperties, executionContext)
          )
        case _ => None
      }
    }
  }

  private def pollEvents(): Future[Seq[Unit]] = {
    Future.sequence(sensors.flatMap {
      case (_, sensor: PollSensor) => Option(sensor.poll())
      case _ => None
    }.toSeq)
  }

}
