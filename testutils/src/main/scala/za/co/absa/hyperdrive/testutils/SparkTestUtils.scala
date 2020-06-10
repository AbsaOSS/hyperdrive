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

package za.co.absa.hyperdrive.testutils

import org.apache.spark.sql.types.{ArrayType, StructField, StructType}

import scala.annotation.tailrec

private[hyperdrive] object SparkTestUtils {

  def areAllFieldsNullable(schema: StructType): Boolean = {
    allFieldsContainNullability(schema, expectedNullability = true)
  }

  def areAllFieldsNonNullable(schema: StructType): Boolean = {
    allFieldsContainNullability(schema, expectedNullability = false)
  }

  private def allFieldsContainNullability(schema: StructType, expectedNullability: Boolean): Boolean = {
    schema.fields.forall(isExpectedNullability(_, expectedNullability))
  }

  private def isExpectedNullability(field: StructField, expectedNullability: Boolean): Boolean = {
    field.dataType match {
      case dt: StructType => allFieldsContainNullability(dt, expectedNullability) // makes sure every field contains expected nullability
      case dt: ArrayType => dt.containsNull == expectedNullability && isExpectedNullabilityForAll(dt, expectedNullability) // makes sure the field itself plus every inner field contains expected nullability
      case _ => field.nullable == expectedNullability // makes sure the field contains expected nullability
    }
  }

  @tailrec
  private def isExpectedNullabilityForAll(arrayType: ArrayType, expectedNullability: Boolean): Boolean = {
    arrayType.elementType match {
      case dt: ArrayType => isExpectedNullabilityForAll(dt, expectedNullability)
      case dt: StructType => allFieldsContainNullability(dt, expectedNullability)
    }
  }
}
