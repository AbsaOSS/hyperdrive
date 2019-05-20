package core.api.rest.services

import core.HyperDriverManager

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

trait AdminService {
  def isManagerRunning: Future[Boolean]
  def startManager: Future[Boolean]
  def stopManager: Future[Boolean]
}

class AdminServiceImpl() extends AdminService {

  override def isManagerRunning: Future[Boolean] = {
    Future.successful(HyperDriverManager.isManagerRunning)
  }

  override def startManager: Future[Boolean] = {
    Future.successful(HyperDriverManager.startManager).map(_=>true)
  }

  override def stopManager: Future[Boolean] = {
    HyperDriverManager.stopManager.map(_=>true)
  }

}
