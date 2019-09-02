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
    val newSchema = setSchemaNullability(df.schema, desiredNullability = true)
    df.sparkSession.createDataFrame(df.rdd, newSchema)
  }

  private def setSchemaNullability(schema: StructType, desiredNullability: Boolean): StructType = {
    StructType(schema.fields.map(field =>
      field.dataType match {
        case dt: StructType =>
          StructField(field.name, setSchemaNullability(dt, desiredNullability), nullable = desiredNullability, field.metadata)
        case dt: ArrayType =>
          StructField(field.name, setArraySchemaNullability(dt, desiredNullability), nullable = desiredNullability, field.metadata)
        case dt =>
          StructField(field.name, dt, nullable = desiredNullability, field.metadata)
      }
    ))
  }

  private def setArraySchemaNullability(arrayType: ArrayType, desiredNullability: Boolean): ArrayType = {
    arrayType.elementType match {
      case dt: ArrayType =>
        ArrayType(setArraySchemaNullability(dt, desiredNullability), containsNull = desiredNullability)
      case dt: StructType =>
        ArrayType(setSchemaNullability(dt, desiredNullability), containsNull = desiredNullability)
      case dt =>
        ArrayType(dt, containsNull = desiredNullability)
    }
  }
}
