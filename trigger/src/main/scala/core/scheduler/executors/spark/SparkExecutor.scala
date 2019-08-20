package core.scheduler.executors.spark

import core.models.{JobInstance, JobParameters}

import scala.concurrent.{ExecutionContext, Future}
import java.util.UUID.randomUUID

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

import scala.collection.JavaConverters._
import org.apache.spark.launcher.SparkLauncher
import play.api.libs.json.{JsValue, Json}
import play.api.libs.ws.ahc.StandaloneAhcWSClient
import core.models.enums.JobStatuses._
import core.scheduler.executors.Executor
import core.scheduler.utilities.SparkExecutorConfig
import play.api.libs.ws.JsonBodyReadables._
import core.scheduler.executors.spark.{FinalStatuses => YarnFinalStatuses}
import org.slf4j.LoggerFactory

object SparkExecutor extends Executor {
  private val wsClient = StandaloneAhcWSClient()(ActorMaterializer()(ActorSystem()))

  override def execute(jobInstance: JobInstance, updateJob: JobInstance => Future[Unit])
                      (implicit executionContext: ExecutionContext): Future[Unit] = {
    jobInstance.executorJobId match {
      case None => submitJob(jobInstance, updateJob)
      case Some(executorJobId) => updateJobStatus(executorJobId, jobInstance, updateJob)
    }
  }

  private def submitJob(jobInstance: JobInstance, updateJob: JobInstance => Future[Unit])
                       (implicit executionContext: ExecutionContext): Future[Unit] = {
    val id = randomUUID().toString
    val ji = jobInstance.copy(executorJobId = Some(id), jobStatus = Submitting)
    updateJob(ji).map { _ =>
      val running = getSparkLauncher(id, ji.jobName, ji.jobParameters).launch()
      Thread.sleep(SparkExecutorConfig.getSubmitTimeOut)
      running.destroyForcibly()
    }
  }

  private def updateJobStatus(executorJobId: String, jobInstance: JobInstance, updateJob: JobInstance => Future[Unit])
                             (implicit executionContext: ExecutionContext): Future[Unit] = {
    wsClient.url(getStatusUrl(executorJobId)).get().map { response =>
      (Json.fromJson[AppsResponse](response.body[JsValue]).asOpt match {
        case Some(asd) => asd.apps.app
        case None => Seq.empty
      }) match {
        case Seq(first) => updateJob(jobInstance.copy(jobStatus = getStatus(first.finalStatus)))
        case _ => updateJob(jobInstance.copy(jobStatus = Lost))
      }
    }
  }

  private def getSparkLauncher(id: String, jobName: String, jobParameters: JobParameters): SparkLauncher = {
    val sparkParameters = SparkParameters(jobParameters)

    val sparkLauncher = new SparkLauncher(Map(
      "HADOOP_CONF_DIR" -> SparkExecutorConfig.getHadoopConfDir,
      "SPARK_PRINT_LAUNCH_COMMAND" -> "1"
    ).asJava)
      .setMaster(SparkExecutorConfig.getMaster)
      .setDeployMode(sparkParameters.deploymentMode)
      .setMainClass(sparkParameters.mainClass)
      .setAppResource(sparkParameters.jobJar)
      .setSparkHome(SparkExecutorConfig.getSparkHome)
      .setAppName(jobName)
      .setConf("spark.yarn.tags", id)
      .addAppArgs(sparkParameters.appArguments.toSeq:_*)
      .addSparkArg("--verbose")
      .redirectToLog(LoggerFactory.getLogger(s"SparkExecutor.executorJobId=$id").getName)
    SparkExecutorConfig.getFilesToDeploy.foreach(file => sparkLauncher.addFile(file))
    SparkExecutorConfig.getAdditionalConfs.foreach(conf => sparkLauncher.setConf(conf._1, conf._2))

    sparkLauncher
  }

  private def getStatusUrl(executorJobId: String): String = {
    s"${SparkExecutorConfig.getHadoopResourceManagerUrlBase}/ws/v1/cluster/apps?applicationTags=$executorJobId"
  }

  private def getStatus(finalStatus: String): JobStatus = {
    finalStatus match {
      case fs if fs == YarnFinalStatuses.Undefined.name => Running
      case fs if fs == YarnFinalStatuses.Succeeded.name => Succeeded
      case fs if fs == YarnFinalStatuses.Failed.name => Failed
      case fs if fs == YarnFinalStatuses.Killed.name => Killed
      case _ => Lost
    }
  }

}