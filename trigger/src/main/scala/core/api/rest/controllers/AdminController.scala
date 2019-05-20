package core.api.rest.controllers

import java.util.concurrent.CompletableFuture

import core.api.rest.services.AdminService
import javax.inject.Inject
import org.springframework.web.bind.annotation._

import scala.compat.java8.FutureConverters._

@RestController
class AdminController @Inject()(adminService: AdminService) {

  @GetMapping(path = Array("/admin/isManager"))
  def isManagerRunning: CompletableFuture[Boolean] = {
    adminService.isManagerRunning.toJava.toCompletableFuture
  }

  @PostMapping(path = Array("/admin/startManager"))
  def startManager(): CompletableFuture[Boolean] = {
    adminService.startManager.toJava.toCompletableFuture
  }

  @PostMapping(path = Array("/admin/stopManager"))
  def stopManager(): CompletableFuture[Boolean] = {
    adminService.stopManager.toJava.toCompletableFuture
  }

}