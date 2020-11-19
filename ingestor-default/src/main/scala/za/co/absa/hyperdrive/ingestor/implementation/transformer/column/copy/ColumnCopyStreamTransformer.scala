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

package za.co.absa.hyperdrive.ingestor.implementation.transformer.column.copy

import org.apache.commons.configuration2.Configuration
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import za.co.absa.hyperdrive.ingestor.api.transformer.{StreamTransformer, StreamTransformerFactory}
import za.co.absa.hyperdrive.ingestor.api.utils.ConfigUtils

/**
 * @param columnsFrom list of columns to copy from. Nested structs can be specified with dots
 * @param columnsTo list of columns to copy from. Nested structs can be specified with dots
 * Example:
 * Given a dataframe with the following schema
 * |-- orig1
 * |    |-- nestedOrig1
 * |-- orig2
 * |    |-- nestedOrig2
 * |-- orig3
 * and the following parameters
 *
 * columnsFrom=Seq("orig1.nestedOrig","orig2")
 * columnsTo=Seq("new1.nested1.nested11","new1.nested2")
 *
 * will produce the following schema
 * |-- orig1
 * |    |-- nestedOrig1
 * |-- orig2
 * |    |-- nestedOrig2
 * |-- orig3
 * |-- new1
 * |    |-- nested1
 * |    |    |-- nested11
 * |    |-- nested2
 * |    |    |-- nestedOrig2
 *
 */
private[transformer] class ColumnCopyStreamTransformer(val columnsFrom: Seq[String], val columnsTo: Seq[String]) extends StreamTransformer {
  case class Node(name: String, copyColumnName: Option[String], children: Seq[Node])

  if (columnsFrom.size != columnsTo.size) {
    throw new IllegalArgumentException("The size of source column names doesn't match the list of target column names " +
      s"${columnsFrom.size} != ${columnsTo.size}.")
  }

  def transform(dataFrame: DataFrame): DataFrame = {
    val parsedColumns = columnsTo.map(to => UnresolvedAttribute.parseAttributeName(to).toList)
    val rootNodeName = "root"
    val rootNode = Node(rootNodeName, None, Seq())
    val copyColumnsList = columnsFrom.zip(parsedColumns.map(rootNodeName :: _))
    val copyColumnsTreeRootNode = copyColumnsList.foldLeft(rootNode)((node, parsedColumn) =>
      createNode(parsedColumn._1, parsedColumn._2, Some(node)))

    copyColumnsTreeRootNode.children.foldLeft(dataFrame)((df, topNode) =>
      df.withColumn(topNode.name, createColumn(topNode)))
  }

  private def createNode(sourceField: String, treePath: List[String], node: Option[Node]): Node = treePath match {
    case last :: Nil => Node(last, Some(sourceField), Seq())
    case head :: tail if node.isDefined && node.get.name == head =>
      Node(head, None, node.get.children.filterNot(_.name == tail.head) :+
        createNode(sourceField, tail, node.get.children.find(_.name == tail.head)))
    case head :: tail => Node(head, None, Seq(createNode(sourceField, tail, None)))
  }

  private def createColumn(node: Node): Column = node.children match {
    case Nil =>
      val originalColumn = node.copyColumnName.getOrElse(
        throw new IllegalStateException(s"Expected a copy column name at leaf node ${node}, got None"))
      val newColumn = node.name
      col(originalColumn).as(newColumn)
    case children => struct(children.map(createColumn):_*).as(node.name)
  }
}

object ColumnCopyStreamTransformer extends StreamTransformerFactory with ColumnCopyStreamTransformerAttributes {
  override def apply(config: Configuration): StreamTransformer = {
    val columnsFrom = ConfigUtils.getSeqOrThrow(KEY_COLUMNS_FROM, config)
    val columnsTo = ConfigUtils.getSeqOrThrow(KEY_COLUMNS_TO, config)
    LogManager.getLogger.info(s"Going to create ColumnRenamingStreamTransformer using: " +
      s"columnsFrom='${columnsFrom.mkString(",")}', columnsTo='${columnsTo.mkString(",")}'")
    new ColumnCopyStreamTransformer(columnsFrom, columnsTo)
  }
}
