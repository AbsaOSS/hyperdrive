package core.scheduler.utilities

import java.util.UUID.randomUUID
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory, ConfigObject}
import core.scheduler.sensors.kafka.KafkaProperties

import scala.collection.JavaConverters._
import scala.util.Try

private object Configs {
  val conf: Config = ConfigFactory.load()

  def getMapFromConf(propertyName: String): Map[String, String] = {
    Try {
      val list = Configs.conf.getObjectList(propertyName).asScala
      (for {
        item: ConfigObject <- list
        entry <- item.entrySet().asScala
        key = entry.getKey
        value = entry.getValue.unwrapped().toString
      } yield (key, value)).toMap
    }.getOrElse(Map.empty[String, String])
  }
}

object KafkaConfig {
  def getConsumerProperties(kafkaProp: KafkaProperties): Properties = {
    val properties = new Properties()
    properties.put("bootstrap.servers", kafkaProp.servers.mkString(","))
    properties.put("group.id", randomUUID().toString)
    properties.put("key.deserializer", Configs.conf.getString("kafkaSource.key.deserializer"))
    properties.put("value.deserializer", Configs.conf.getString("kafkaSource.value.deserializer"))
    properties.put("max.poll.records", Configs.conf.getString("kafkaSource.max.poll.records"))

    Configs.getMapFromConf("kafkaSource.properties").foreach { case (key, value)  =>
      properties.put(key, value)
    }

    properties
  }

  def getPollDuration: Long =
    Configs.conf.getLong("kafkaSource.poll.duration")
}

object SensorsConfig {
  def getThreadPoolSize: Int =
    Configs.conf.getInt("scheduler.sensors.thread.pool.size")
}

object SchedulerConfig {
  def getHeartBeat: Int =
    Configs.conf.getInt("scheduler.heart.beat")
  def getMaxParallelJobs: Int =
    Configs.conf.getInt("scheduler.jobs.parallel.number")
}

object ExecutorsConfig {
  def getThreadPoolSize: Int =
    Configs.conf.getInt("scheduler.executors.thread.pool.size")
}

object SparkExecutorConfig {
  def getSubmitTimeOut: Int =
    Configs.conf.getInt("sparkYarnSink.submitTimeout")
  def getHadoopConfDir: String =
    Configs.conf.getString("sparkYarnSink.hadoopConfDir")
  def getMaster: String =
    Configs.conf.getString("sparkYarnSink.master")
  def getSparkHome: String =
    Configs.conf.getString("sparkYarnSink.sparkHome")
  def getHadoopResourceManagerUrlBase: String =
    Configs.conf.getString("sparkYarnSink.hadoopResourceManagerUrlBase")
  def getFilesToDeploy: Seq[String] =
    Try(Configs.conf.getStringList("sparkYarnSink.filesToDeploy").asScala).getOrElse(Seq.empty[String])
  def getAdditionalConfs: Map[String, String] =
    Configs.getMapFromConf("sparkYarnSink.additionalConfs")
}