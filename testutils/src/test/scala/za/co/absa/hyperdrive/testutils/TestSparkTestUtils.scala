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

import org.apache.spark.sql.types._
import org.scalatest.FunSpec

class TestSparkTestUtils extends FunSpec {

  private val flatTypeTrueNullability = new StructType()
    .add(StructField("a", IntegerType, nullable = true))
    .add(StructField("b", LongType, nullable = true))

  private val flatTypeFalseNullability = new StructType()
    .add(StructField("c", IntegerType, nullable = false))
    .add(StructField("d", LongType, nullable = false))

  private val nestedTypeTrueNullability = new StructType()
    .add(StructField("e", ArrayType(flatTypeTrueNullability, containsNull = true), nullable = true))

  private val nestedTypeFalseNullability = new StructType()
    .add(StructField("f", ArrayType(flatTypeFalseNullability, containsNull = false), nullable = false))

  private val nestedTrueButOthersFalse = new StructType()
    .add(StructField("g", ArrayType(flatTypeFalseNullability, containsNull = true), nullable = false))

  describe("areAllFieldsNonNullable") {

    import SparkTestUtils.areAllFieldsNonNullable

    it("should check the nullability of the outermost fields") {
      assert(!areAllFieldsNonNullable(flatTypeTrueNullability))
      assert(areAllFieldsNonNullable(flatTypeFalseNullability))
    }

    it("should check the nullability of the nested fields") {
      assert(!areAllFieldsNonNullable(nestedTypeTrueNullability))
      assert(areAllFieldsNonNullable(nestedTypeFalseNullability))
    }

    it("should return false if the innermost field nullability does not comply") {
      assert(!areAllFieldsNonNullable(nestedTrueButOthersFalse))
    }
  }

  describe("areAllFieldsNullable") {

    import SparkTestUtils.areAllFieldsNullable

    it("should check the nullability of the outermost fields") {
      assert(areAllFieldsNullable(flatTypeTrueNullability))
      assert(!areAllFieldsNullable(flatTypeFalseNullability))
    }

    it("should check the nullability of the nested fields") {
      assert(areAllFieldsNullable(nestedTypeTrueNullability))
      assert(!areAllFieldsNullable(nestedTypeFalseNullability))
    }

    it("should return false if the innermost field nullability does not comply") {
      assert(!areAllFieldsNullable(nestedTrueButOthersFalse))
    }
  }
}
