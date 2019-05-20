package core.api.rest

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import core.api.rest.services._
import core.models.enums.EventTypes.EventType
import core.models.enums.JobStatuses.JobStatus
import core.models.enums.{EventTypes, JobStatuses, JobTypes}
import core.models.enums.JobTypes.JobType
import core.persistance.{JobInstanceRepositoryImpl, WorkflowRepositoryImpl}
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.{Bean, Configuration}
import org.springframework.scheduling.annotation.EnableAsync
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor

@SpringBootApplication
@EnableAsync
@Configuration
class Application() {

  @Bean def asyncExecutor(): ThreadPoolTaskExecutor = {
    val executor = new ThreadPoolTaskExecutor()
    executor.setCorePoolSize(12)
    executor.setMaxPoolSize(24)
    executor.setQueueCapacity(1024)
    executor.initialize()
    executor
  }

  @Bean
  def objectMapper(): ObjectMapper = {
    val module = new SimpleModule()
      .addDeserializer(classOf[EventType], new EventTypesDeserializer)
      .addDeserializer(classOf[JobStatus], new JobStatusesDeserializer)
      .addDeserializer(classOf[JobType], new JobTypesDeserializer)

    new ObjectMapper()
      .registerModule(DefaultScalaModule)
      .registerModule(new JavaTimeModule())
      .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      .configure(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES, false)
      .registerModule(module)
  }

  @Bean
  def getJobInstanceService: JobInstanceService = new JobInstanceServiceImpl(new JobInstanceRepositoryImpl)

  @Bean
  def getWorkflowService: WorkflowService = new WorkflowServiceImpl(new WorkflowRepositoryImpl)

  @Bean
  def getAdminService: AdminService = new AdminServiceImpl()

  class EventTypesDeserializer extends JsonDeserializer[EventType] {
    override def deserialize(p: JsonParser, ctxt: DeserializationContext): EventType = {
      val node = p.getCodec.readTree[JsonNode](p)
      val value = node.get("name").textValue()
      EventTypes.eventTypes.find(_.name == value).getOrElse(throw new Exception("Failed to find enum value"))
    }
  }

  class JobStatusesDeserializer extends JsonDeserializer[JobStatus] {
    override def deserialize(p: JsonParser, ctxt: DeserializationContext): JobStatus = {
      val node = p.getCodec.readTree[JsonNode](p)
      val value = node.get("name").textValue()
      JobStatuses.statuses.find(_.name == value).getOrElse(throw new Exception("Failed to find enum value"))
    }
  }

  class JobTypesDeserializer extends JsonDeserializer[JobType] {
    override def deserialize(p: JsonParser, ctxt: DeserializationContext): JobType = {
      val node = p.getCodec.readTree[JsonNode](p)
      val value = node.get("name").textValue()
      JobTypes.jobTypes.find(_.name == value).getOrElse(throw new Exception("Failed to find enum value"))
    }
  }
}

object Application extends App {
  SpringApplication.run(classOf[Application])
}