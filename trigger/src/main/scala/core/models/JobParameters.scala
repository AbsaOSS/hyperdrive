package core.models

case class JobParameters(
  variables: Map[String, String],
  maps: Map[String, Set[String]]
)