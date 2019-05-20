package core.scheduler.utilities

import java.time.Duration

import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.collection.mutable

object KafkaRichConsumer {

  implicit class RichKafkaConsumer[A, B](kafkaConsumer: KafkaConsumer[A, B]) extends {

    def seek(): Unit = {
      for {
        topicPartition <- kafkaConsumer.assignmentAsScala()
        pollPosition = kafkaConsumer.position(topicPartition)
        committedPosition = kafkaConsumer.committed(topicPartition).offset()
        if pollPosition > committedPosition
      } yield {
        kafkaConsumer.seek(topicPartition, committedPosition)
      }
    }

    def pollAsScala(timeout: Duration): Iterable[ConsumerRecord[A, B]] = kafkaConsumer.poll(timeout).asScala

    def assignmentAsScala(): mutable.Set[TopicPartition] = kafkaConsumer.assignment().asScala

  }

}
