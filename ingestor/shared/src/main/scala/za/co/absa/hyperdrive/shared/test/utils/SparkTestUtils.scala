/*
 *  Copyright 2019 ABSA Group Limited
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package za.co.absa.hyperdrive.shared.test.utils

import org.apache.spark.sql.types.{ArrayType, StructField, StructType}

object SparkTestUtils {

  def areAllFieldsNullable(schema: StructType): Boolean = {
    schema.fields.forall(isNullable(_))
  }

  private def isNullable(field: StructField): Boolean = {
    val inspectionResult = field match {
      case dt: StructType => areAllFieldsNullable(dt)
      case dt: ArrayType => areAllFieldsNullable(dt)
      case dt: StructField => dt.nullable
    }

    inspectionResult && field.nullable
  }

  private def areAllFieldsNullable(arrayType: ArrayType): Boolean = {
    arrayType.elementType match {
      case dt: ArrayType => dt.containsNull && areAllFieldsNullable(dt)
      case dt: StructType => areAllFieldsNullable(dt)
      case dt: StructField => dt.nullable
    }
  }
}
