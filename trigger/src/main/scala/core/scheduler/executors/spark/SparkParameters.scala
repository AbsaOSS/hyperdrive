package core.scheduler.executors.spark

import core.models.JobParameters

case class SparkParameters(
  jobJar: String,
  mainClass: String,
  deploymentMode: String,
  appArguments: Set[String]
)

object SparkParameters {
  def apply(jobParameters: JobParameters): SparkParameters = {
    SparkParameters(
      jobJar = jobParameters.variables("jobJar"),
      mainClass = jobParameters.variables("mainClass"),
      deploymentMode = jobParameters.variables("deploymentMode"),
      appArguments = jobParameters.maps("appArguments")
    )
  }
}

