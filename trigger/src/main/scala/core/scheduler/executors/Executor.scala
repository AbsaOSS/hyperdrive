package core.scheduler.executors

import core.models.{JobInstance, TriggerProperties}

import scala.concurrent.{ExecutionContext, Future}

trait Executor {
  def execute(jobInstance: JobInstance, updateJob: JobInstance => Future[Unit])(implicit executionContext: ExecutionContext): Future[Unit]
}
