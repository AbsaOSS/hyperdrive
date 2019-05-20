package core.scheduler.executors.spark

import play.api.libs.json.{Json, OFormat}

case class App(
                id: String,
                name: String,
                state: String,
                finalStatus: String
              )

case class Apps(app: Seq[App])

case class AppsResponse(apps: Apps)

object AppsResponse {
  implicit val appFormat: OFormat[App] = Json.format[App]
  implicit val appsFormat: OFormat[Apps] = Json.format[Apps]
  implicit val appsResponseFormat: OFormat[AppsResponse] = Json.format[AppsResponse]
}

object FinalStatuses {

  sealed abstract class FinalStatus(val name: String) {
    override def toString: String = name
  }

  case object Undefined extends FinalStatus("UNDEFINED")
  case object Succeeded extends FinalStatus("SUCCEEDED")
  case object Failed extends FinalStatus("FAILED")
  case object Killed extends FinalStatus("KILLED")

  val finalStatuses: Set[FinalStatus] = Set(Undefined, Succeeded, Failed, Killed)

}