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

package za.co.absa.hyperdrive.ingestor.implementation.transformer.column.selection

import org.apache.spark.sql.DataFrame
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar
import za.co.absa.hyperdrive.ingestor.api.transformer.StreamTransformer

class TestColumnSelectorStreamTransformer extends AnyFlatSpec with MockitoSugar {

  behavior of createTransformer(Seq("*")).getClass.getName

  it should "select all columns in DataFrame" in {
    val streamData = mock[DataFrame]
    when(streamData.select("*")).thenReturn(streamData)

    val transformer = createTransformer(Seq("*"))
    assert(streamData == transformer.transform(streamData))
    verify(streamData).select("*")
  }

  it should "select only specified columns" in {
    val columns = Seq("a","b","c")
    val streamData = mock[DataFrame]
    when(streamData.select(columns.head, columns.tail:_*)).thenReturn(streamData)

    val transformer = createTransformer(columns)
    assert(streamData == transformer.transform(streamData))
    verify(streamData).select(columns.head, columns.tail:_*)
  }

  it should "throw on empty list of columns" in {
    assertThrows[IllegalArgumentException](
      new ColumnSelectorStreamTransformer(Seq())
    )
  }

  private def createTransformer(columns: Seq[String]): StreamTransformer = new ColumnSelectorStreamTransformer(columns)
}
