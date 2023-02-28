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

import org.apache.commons.configuration2.DynamicCombinedConfiguration
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructType}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.spark.commons.test.SparkTestBase

class TestColumnCopyStreamTransformer extends AnyFlatSpec with SparkTestBase with Matchers with BeforeAndAfter {

  it should "copy columns while leaving existing columns intact" in {
    // given
    val inputSchema = new StructType()
      .add("col1Top", new StructType()
        .add("Field.1", StringType, nullable = false)
      , nullable = false)
      .add("col2Top", new StructType()
        .add("Field2", StringType)
        .add("Field3", new ArrayType(IntegerType, containsNull = true)))
      .add("col3Top", StringType)
    val memoryStream = new MemoryStream[Row](1, spark.sqlContext)(RowEncoder(inputSchema))
    val df = memoryStream.toDF()

    val config = new DynamicCombinedConfiguration()
    config.setListDelimiterHandler(new DefaultListDelimiterHandler(','))
    config.addProperty(ColumnCopyStreamTransformer.KEY_COLUMNS_FROM,
      "col1Top.`Field.1`, col2Top, col2Top, col2Top.Field2, col2Top.Field3")
    config.addProperty(ColumnCopyStreamTransformer.KEY_COLUMNS_TO,
      "copy1.`cp.col1Top`.cpField1, copy1.cpCol2Top, copy2, copy2.l1.l2.l3.Field2, copy2.l1.l2.Field3")

    // when
    val resultDf = ColumnCopyStreamTransformer(config).transform(df)

    // then
    val expectedSchema = new StructType()
      .add("col1Top", new StructType()
        .add("Field.1", StringType, nullable = false)
      , nullable = false)
      .add("col2Top", new StructType()
        .add("Field2", StringType)
        .add("Field3", new ArrayType(IntegerType, containsNull = true)))
      .add("col3Top", StringType)
      .add("copy1", new StructType()
        .add("cp.col1Top", new StructType()
          .add("cpField1", StringType, nullable = false),
          nullable = false)
        .add("cpCol2Top", new StructType()
          .add("Field2", StringType)
          .add("Field3", new ArrayType(IntegerType, containsNull = true))),
        nullable = false)
      .add("copy2", new StructType()
        .add("l1", new StructType()
          .add("l2", new StructType()
            .add("l3", new StructType()
              .add("Field2", StringType)
              , nullable = false)
            .add("Field3", new ArrayType(IntegerType, containsNull = true))
            , nullable = false)
          , nullable = false)
        , nullable = false)
    resultDf.schema shouldBe expectedSchema
  }

  it should "throw an exception if columns from do not match columns to" in {
    val config = new DynamicCombinedConfiguration()
    config.setListDelimiterHandler(new DefaultListDelimiterHandler(','))
    config.addProperty(ColumnCopyStreamTransformer.KEY_COLUMNS_FROM, "col1,col2")
    config.addProperty(ColumnCopyStreamTransformer.KEY_COLUMNS_TO, "col1")

    val ex = the[IllegalArgumentException] thrownBy ColumnCopyStreamTransformer(config)
    ex.getMessage should include("The size of source column names doesn't match")
  }
}
