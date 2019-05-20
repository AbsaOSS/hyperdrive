package core.models.enums

object JobStatuses {

  sealed abstract class JobStatus(val name: String, val isFinalStatus: Boolean, val isFailed: Boolean, val isRunning: Boolean) {
    override def toString: String = name
  }

  case object InQueue extends JobStatus("InQueue", false, false, false)
  case object Submitting extends JobStatus("Submitting", false, false, true)
  case object SubmitFail extends JobStatus("SubmitFail", true, true, false)
  case object Running extends JobStatus("Running", false, false, true)
  case object Lost extends JobStatus("Lost", true, true, false)
  case object Succeeded extends JobStatus("Succeeded", true, false, false)
  case object Failed extends JobStatus("Failed", true, true, false)
  case object Killed extends JobStatus("Killed", true, true, false)
  case object InvalidExecutor extends JobStatus("InvalidExecutor", true, true, false)

  val statuses: Set[JobStatus] = Set(InQueue,Submitting,SubmitFail,Running,Lost,Succeeded,Failed,Killed, InvalidExecutor)
  val finalStatuses: Set[JobStatus] = statuses.filter(!_.isFinalStatus)
  val nonFinalStatuses: Set[JobStatus] = statuses.filter(_.isFinalStatus)
}