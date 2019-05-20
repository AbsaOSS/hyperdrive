package core.models

case class OverallStatistics(
  successful: Int,
  failed: Int,
  running: Int,
  queued: Int
)

case class PerWorkflowStatistics(
  jobDefinitionId: Long,
  workflowName: String,
  isActive: Boolean,
  total: Int,
  successful: Int,
  failed: Int,
  queued: Int,
  running: Int
)