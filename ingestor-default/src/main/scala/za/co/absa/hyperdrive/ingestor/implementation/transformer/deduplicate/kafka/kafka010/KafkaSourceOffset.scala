
package za.co.absa.hyperdrive.ingestor.implementation.transformer.deduplicate.kafka.kafka010

import org.apache.kafka.common.TopicPartition
import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset}
import org.apache.spark.sql.sources.v2.reader.streaming.PartitionOffset
/**
 * An [[Offset]] for the [[KafkaSource]]. This one tracks all partitions of subscribed topics and
 * their offsets.
 */
case class KafkaSourceOffset(partitionToOffsets: Map[TopicPartition, Long]) extends Offset {

  override val json = JsonUtils.partitionOffsets(partitionToOffsets)
}

private[kafka]
case class KafkaSourcePartitionOffset(topicPartition: TopicPartition, partitionOffset: Long)
  extends PartitionOffset

/** Companion object of the [[KafkaSourceOffset]] */
private[kafka] object KafkaSourceOffset {

  def getPartitionOffsets(offset: Offset): Map[TopicPartition, Long] = {
    offset match {
      case o: KafkaSourceOffset => o.partitionToOffsets
      case so: SerializedOffset => KafkaSourceOffset(so).partitionToOffsets
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid conversion from offset of ${offset.getClass} to KafkaSourceOffset")
    }
  }

  /**
   * Returns [[KafkaSourceOffset]] from a variable sequence of (topic, partitionId, offset)
   * tuples.
   */
  def apply(offsetTuples: (String, Int, Long)*): KafkaSourceOffset = {
    KafkaSourceOffset(offsetTuples.map { case(t, p, o) => (new TopicPartition(t, p), o) }.toMap)
  }

  /**
   * Returns [[KafkaSourceOffset]] from a JSON [[SerializedOffset]]
   */
  def apply(offset: SerializedOffset): KafkaSourceOffset =
    KafkaSourceOffset(JsonUtils.partitionOffsets(offset.json))
}
