/*
 *
 * Copyright 2019 ABSA Group Limited
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package za.co.absa.hyperdrive.transformer.data.impl

import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec
import org.scalatest.mockito.MockitoSugar
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._

class TestSelectAllStreamTransformer extends FlatSpec with MockitoSugar {

  private val transformer = new SelectAllStreamTransformer

  behavior of transformer.getClass.getName

  it should "throw on null DataFrame" in {
    assertThrows[IllegalArgumentException](
      transformer.transform(streamData = null)
    )
  }

  it should "select all columns in DataFrame" in {
    val streamData = mock[DataFrame]
    when(streamData.select("*")).thenReturn(streamData)

    assert(streamData == transformer.transform(streamData))
    verify(streamData.select("*"))
  }
}
