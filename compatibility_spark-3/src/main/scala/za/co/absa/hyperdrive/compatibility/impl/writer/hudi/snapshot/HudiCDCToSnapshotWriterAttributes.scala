
package za.co.absa.hyperdrive.compatibility.impl.writer.hudi.snapshot

import za.co.absa.hyperdrive.ingestor.api.writer.StreamWriterCommonAttributes
import za.co.absa.hyperdrive.ingestor.api.{HasComponentAttributes, PropertyMetadata}

trait HudiCDCToSnapshotWriterAttributes extends HasComponentAttributes {
  private val rootFactoryConfKey = "writer.hudicdctosnapshot"
  val KEY_DESTINATION_DIRECTORY = s"$rootFactoryConfKey.destination.directory"
  val KEY_EXTRA_CONFS_ROOT = s"$rootFactoryConfKey.options"
  val KEY_PARTITION_COLUMNS = s"$rootFactoryConfKey.partition.columns"
  val KEY_KEY_COLUMN = s"$rootFactoryConfKey.key.column"
  val KEY_OPERATION_COLUMN = s"$rootFactoryConfKey.operation.column"
  val KEY_OPERATION_DELETED_VALUES = s"$rootFactoryConfKey.operation.deleted.values"
  val KEY_PRECOMBINE_COLUMN = s"$rootFactoryConfKey.precombineColumn"

  override def getName: String = "Delta Stream Writer"

  override def getDescription: String = "This writer saves ingested data in Delta format on a filesystem (e.g. HDFS)"

  override def getProperties: Map[String, PropertyMetadata] = Map(
    KEY_DESTINATION_DIRECTORY -> PropertyMetadata("Destination directory", Some("A path to a directory"), required = true),
    KEY_PARTITION_COLUMNS -> PropertyMetadata("Partition columns", Some("Comma-separated list of columns to partition by"), required = false),
    KEY_KEY_COLUMN -> PropertyMetadata("Key column", Some("A column with unique entity identifier"), required = true),
    KEY_OPERATION_COLUMN -> PropertyMetadata("Operation column", Some("A column containing value marking a record with an operation"), required = true),
    KEY_OPERATION_DELETED_VALUES -> PropertyMetadata("Delete value for Operation column", Some("Values marking a record for deletion in the operation column"), required = true),
    KEY_PRECOMBINE_COLUMN -> PropertyMetadata("Precombine columns", Some("When two records have the same key value, we will pick the one with the largest value for precombine columns. Evaluated in provided order"), required = true),
    StreamWriterCommonAttributes.keyTriggerProcessingTime -> StreamWriterCommonAttributes.triggerProcessingTimeMetadata,
    StreamWriterCommonAttributes.keyCheckpointBaseLocation -> StreamWriterCommonAttributes.checkpointBaseLocation
  )
}
