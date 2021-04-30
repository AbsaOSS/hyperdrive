/*
 * Copyright 2018 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.hyperdrive.ingestor.implementation.utils

import java.net.URI

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.{DataSource, HadoopFsRelation}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import za.co.absa.hyperdrive.compatibility.provider.CompatibleSparkUtilProvider
import za.co.absa.hyperdrive.shared.utils.FileUtils

import scala.util.{Failure, Success, Try}

object MetadataLogUtil {
  def getParquetFilesNotListedInMetadataLog(spark: SparkSession, rootPath: String): Try[Set[String]] = {
    implicit val fs: FileSystem = FileSystem.get(new URI(rootPath), spark.sparkContext.hadoopConfiguration)
    if(FileUtils.notExists(rootPath) || FileUtils.isEmpty(rootPath)) {
      Success(Set.empty)
    } else {
      for {
        fileSystemFiles <- getFileSystemFiles(spark, rootPath)
        metadataLogFiles <- getMetadataLogFiles(spark, rootPath)
      } yield fileSystemFiles.diff(metadataLogFiles)
    }
  }

  private def getFileSystemFiles(spark: SparkSession, rootPath: String): Try[Set[String]] = {
    val dummySchemaToAvoidSchemaInference = new StructType()
      .add(StructField("dummy", IntegerType, nullable = true))
    val parquetDataSource = DataSource(
      spark,
      classOf[ParquetFileFormat].getCanonicalName,
      Seq(s"$rootPath/*"),
      userSpecifiedSchema = Some(dummySchemaToAvoidSchemaInference)
    )
    val fileSystemRelation = parquetDataSource.resolveRelation().asInstanceOf[HadoopFsRelation]
    val parquetFilesArr = fileSystemRelation.location.inputFiles
    val parquetFiles = parquetFilesArr.toSet
    if (parquetFiles.size != parquetFilesArr.length) {
      Failure(new IllegalStateException("Parquet file paths on filesystem are not unique"))
    }

    Success(parquetFiles)
  }

  private def getMetadataLogFiles(spark: SparkSession, rootPath: String): Try[Set[String]] = {
    val metadataLogFileIndex = CompatibleSparkUtilProvider.createMetadataLogFileIndex(spark, rootPath)
    val parquetFilesArr = metadataLogFileIndex.inputFiles
    val parquetFiles = parquetFilesArr.toSet
    if (parquetFiles.size != parquetFilesArr.length) {
      Failure(new IllegalStateException("Parquet file paths in metadata log are not unique"))
    }

    Success(parquetFiles)
  }
}
