package core

import core.persistance.{EventRepositoryImpl, EventTriggersRepositoryImpl, JobDefinitionsRepositoryImpl, JobInstanceRepositoryImpl}
import core.scheduler.JobScheduler
import core.scheduler.eventProcessor.EventProcessor
import core.scheduler.executors.Executors
import core.scheduler.sensors.Sensors

import scala.concurrent.Future

object HyperDriverManager {

  var jobScheduler: JobScheduler = initialize()

  def isManagerRunning: Boolean = {
    this.jobScheduler.isManagerRunning
  }

  def startManager: Unit = {
    this.jobScheduler.startManager()
  }

  def stopManager: Future[Unit] = {
    this.jobScheduler.stopManager()
  }

  private def initialize(): JobScheduler = {
    new JobScheduler(
      sensors = new Sensors(
        new EventProcessor(
          new EventRepositoryImpl(),
          new JobDefinitionsRepositoryImpl(),
          new JobInstanceRepositoryImpl()
        ),
        new EventTriggersRepositoryImpl()
      ),
      executors = new Executors(new JobInstanceRepositoryImpl()),
      jobInstanceRepository = new JobInstanceRepositoryImpl()
    )
  }

}
