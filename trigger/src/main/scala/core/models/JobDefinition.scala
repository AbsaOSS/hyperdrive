package core.models

import core.models.enums.JobTypes.JobType

case class JobDefinition(
  workflowId: Long,
  name: String,
  jobType: JobType,
  jobParameters: JobParameters,
  id: Long = 0
)