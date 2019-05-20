package core.scheduler

import java.util.concurrent
import java.util.concurrent.atomic.AtomicBoolean

import core.persistance._
import core.scheduler.executors.Executors
import core.scheduler.sensors.Sensors
import core.scheduler.utilities.{SchedulerConfig, SensorsConfig}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}
import org.slf4j.LoggerFactory

class JobScheduler(sensors: Sensors, executors: Executors, jobInstanceRepository: JobInstanceRepository) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val HEART_BEAT: Int = SchedulerConfig.getHeartBeat
  val NUM_OF_PAR_TASKS: Int = SchedulerConfig.getMaxParallelJobs

  private implicit val executionContext: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(concurrent.Executors.newFixedThreadPool(SensorsConfig.getThreadPoolSize))

  private val isManagerRunningAtomic = new AtomicBoolean(false)
  private var runningScheduler: Future[Unit] = Future.successful((): Unit)
  private var runningSensors = Future.successful((): Unit)
  private var runningEnqueue = Future.successful((): Unit)
  private val runningJobs = mutable.Map.empty[Long, Future[Unit]]

  def startManager(): Unit = {
    if(!isManagerRunningAtomic.get() && runningScheduler.isCompleted) {
      isManagerRunningAtomic.set(true)
      runningScheduler =
        Future {
          while (isManagerRunningAtomic.get()) {
            logger.debug("Running manager heart beat.")
            removeFinishedJobs()
            processEvents()
            enqueueJobs()
            Thread.sleep(HEART_BEAT)
          }
        }
      runningScheduler.onComplete {
        case Success(_) =>
          logger.debug("Manager stopped.")
        case Failure(exception) =>
          logger.debug(s"Manager stopped with exception.", exception)
      }
    }
  }

  def stopManager(): Future[Unit] = {
    isManagerRunningAtomic.set(false)
    runningScheduler
  }

  def isManagerRunning: Boolean = {
    !runningScheduler.isCompleted
  }

  private def enqueueJobs(emptySlotsSize: Int): Future[Unit] = {
    jobInstanceRepository.getNewActiveJobs(runningJobs.keys.toSeq, emptySlotsSize).map {
      _.foreach { job =>
        logger.debug(s"Deploying job = ${job.id}")
        runningJobs.put(job.id, executors.executeJob(job))
      }
    }
  }

  private def removeFinishedJobs(): Unit = {
    if(runningEnqueue.isCompleted){
      runningJobs.foreach {
        case (id, fut) if fut.isCompleted => runningJobs.remove(id)
        case _ => ()
      }
    }
  }

  private def processEvents(): Unit = {
    if(runningSensors.isCompleted){
      runningSensors = sensors.processEvents()
      runningSensors.onComplete {
        case Success(_) =>
          logger.debug("Running sensors finished successfully.")
        case Failure(exception) =>
          logger.debug(s"Running sensors finished with exception.", exception)
      }
    }
  }

  private def enqueueJobs(): Unit = {
    if(runningJobs.size < NUM_OF_PAR_TASKS && runningEnqueue.isCompleted){
      runningEnqueue = enqueueJobs(Math.max(0, NUM_OF_PAR_TASKS - runningJobs.size))
      runningEnqueue.onComplete {
        case Success(_) =>
          logger.debug("Running enqueue finished successfully.")
        case Failure(exception) =>
          logger.debug(s"Running enqueue finished with exception.", exception)
      }
    }
  }

}