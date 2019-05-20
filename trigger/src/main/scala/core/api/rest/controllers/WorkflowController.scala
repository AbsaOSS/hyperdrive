package core.api.rest.controllers

import java.util.concurrent.CompletableFuture

import core.api.rest.services.WorkflowService
import core.models.{Workflow, WorkflowJoined, WorkflowState}
import javax.inject.Inject
import org.springframework.web.bind.annotation._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.compat.java8.FutureConverters._

@RestController
class WorkflowController @Inject()(workflowService: WorkflowService) {

  @PutMapping(path = Array("/workflow"))
  def createWorkflow(@RequestBody workflow: WorkflowJoined): CompletableFuture[Boolean] = {
    workflowService.createWorkflow(workflow).toJava.toCompletableFuture
  }

  @GetMapping(path = Array("/workflow"))
  def getWorkflow(@RequestParam id: Long): CompletableFuture[Option[WorkflowJoined]] = {
    workflowService.getWorkflow(id).toJava.toCompletableFuture
  }

  @GetMapping(path = Array("/workflows"))
  def getWorkflows(): CompletableFuture[Seq[Workflow]] = {
    workflowService.getWorkflows.toJava.toCompletableFuture
  }

  @DeleteMapping(path = Array("/workflows"))
    def deleteWorkflow(@RequestParam id: Long): CompletableFuture[Boolean] = {
      workflowService.deleteWorkflow(id).toJava.toCompletableFuture
    }

  @PostMapping(path = Array("/workflows"))
  def updateWorkflow(@RequestBody workflow: WorkflowJoined): CompletableFuture[Boolean] = {
    workflowService.updateWorkflow(workflow).toJava.toCompletableFuture
  }

  @PostMapping(path = Array("/workflows/{id}/setActiveState"))
  def updateWorkflowActiveState(@PathVariable id: Long, @RequestBody workflowState: WorkflowState): CompletableFuture[Boolean] = {
    workflowService.updateWorkflowActiveState(id, workflowState.isActive).toJava.toCompletableFuture
  }

}