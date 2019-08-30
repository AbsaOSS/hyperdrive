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

package za.co.absa.hyperdrive.shared.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}

object SparkUtils {

  def setAllColumnsNullable(df: DataFrame): DataFrame = {
    val newSchema = getNullableSchema(df.schema)
    df.sparkSession.createDataFrame(df.rdd, newSchema)
  }

  private def getNullableSchema(schema: StructType): StructType = {
    StructType(schema.fields.map(field =>
      field.dataType match {
        case dt: StructType =>
          StructField(field.name, getNullableSchema(dt), nullable = true, field.metadata)
        case dt: ArrayType =>
          StructField(field.name, getNullableArray(dt), nullable = true, field.metadata)
        case dt =>
          StructField(field.name, dt, nullable = true, field.metadata)
      }
    ))
  }

  private def getNullableArray(arrayType: ArrayType): ArrayType = {
    arrayType.elementType match {
      case dt: ArrayType =>
        ArrayType(getNullableArray(dt), containsNull = true)
      case dt: StructType =>
        ArrayType(getNullableSchema(dt), containsNull = true)
      case dt =>
        ArrayType(dt, containsNull = true)
    }
  }
}
