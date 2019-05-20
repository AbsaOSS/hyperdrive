package core.models.tables

import core.models.enums.{EventTypes, JobStatuses, JobTypes}
import core.models.enums.EventTypes.EventType
import core.models.enums.JobStatuses.JobStatus
import core.models.enums.JobTypes.JobType
import play.api.libs.json.{JsValue, Json}
import slick.jdbc.JdbcType
import scala.util.Try
import core.models.tables.JDBCProfile.profile._

object JdbcTypeMapper {

  implicit val eventTypeMapper: JdbcType[EventType] =
    MappedColumnType.base[EventType, String](
      eventType => eventType.name,
      eventTypeName => EventTypes.eventTypes.find(_.name == eventTypeName).getOrElse(
        throw new Exception(s"Couldn't find EventType: $eventTypeName")
      )
    )

  implicit val jobTypeMapper: JdbcType[JobType] =
    MappedColumnType.base[JobType, String](
      jobType => jobType.name,
      jobTypeName => JobTypes.jobTypes.find(_.name == jobTypeName).getOrElse(
        throw new Exception(s"Couldn't find JobType: $jobTypeName")
      )
    )

  implicit val statusMapper: JdbcType[JobStatus] =
    MappedColumnType.base[JobStatus, String](
      jobStatus => jobStatus.name,
      jobStatusName => JobStatuses.statuses.find(_.name == jobStatusName).getOrElse(
        throw new Exception(s"Couldn't find JobStatus: $jobStatusName")
      )
    )

  implicit val payloadMapper: JdbcType[JsValue] =
    MappedColumnType.base[JsValue, String](
      payload => payload.toString(),
      payloadString => Try(Json.parse(payloadString)).getOrElse(
        throw new Exception(s"Couldn't parse payload: $payloadString")
      )
    )

  //TEMPORARY MAPPING, SEPARATE TABLE WILL BE CREATED
  implicit val mapMapper: JdbcType[Map[String, String]] =
    MappedColumnType.base[Map[String, String], String](
      parameters => Json.toJson(parameters).toString(),
      parametersEncoded => Json.parse(parametersEncoded).as[Map[String, String]]
    )

  //TEMPORARY MAPPING, SEPARATE TABLE WILL BE CREATED
  implicit val mapSetMapper: JdbcType[Map[String, Set[String]]] =
    MappedColumnType.base[Map[String, Set[String]], String](
      parameters => Json.toJson(parameters).toString(),
      parametersEncoded => Json.parse(parametersEncoded).as[Map[String, Set[String]]]
    )

}
