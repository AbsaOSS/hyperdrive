package core.api.rest.controllers

import java.util.concurrent.CompletableFuture

import core.api.rest.services.JobInstanceService
import core.models.{JobInstance, OverallStatistics, PerWorkflowStatistics}
import javax.inject.Inject
import org.springframework.web.bind.annotation._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.compat.java8.FutureConverters._

@RestController
class JobInstanceController @Inject()(jobInstanceService: JobInstanceService) {

  @GetMapping(path = Array("/jobInstances"))
  def getJobInstances(@RequestParam jobDefinitionId: Long): CompletableFuture[Seq[JobInstance]] = {
    jobInstanceService.getJobInstances(jobDefinitionId).toJava.toCompletableFuture
  }

  @GetMapping(path = Array("/jobInstances/overallStatistics"))
  def getOverallStatistics(): CompletableFuture[OverallStatistics] = {
    jobInstanceService.getOverallStatistics().toJava.toCompletableFuture
  }

  @GetMapping(path = Array("/jobInstances/perWorkflowStatistics"))
  def getPerWorkflowStatistics(): CompletableFuture[Seq[PerWorkflowStatistics]] = {
    jobInstanceService.getPerWorkflowStatistics().toJava.toCompletableFuture
  }

}

