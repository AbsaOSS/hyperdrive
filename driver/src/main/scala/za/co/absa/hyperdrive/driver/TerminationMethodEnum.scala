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

package za.co.absa.hyperdrive.driver

import scala.util.{Failure, Success, Try}

object TerminationMethodEnum {
  sealed abstract class TerminationMethod(val name: String) {
    override def toString: String = name
  }

  case object ProcessAllAvailable extends TerminationMethod("ProcessAllAvailable")
  case object AwaitTermination extends TerminationMethod("AwaitTermination")

  val values = Seq(ProcessAllAvailable, AwaitTermination)
  def of(name: String): Try[TerminationMethod] = {
    values.find(p => p.toString.toUpperCase == name.toUpperCase)
      .map(Success(_))
      .getOrElse(Failure(new IllegalArgumentException(s"No TerminationMethod with name $name exists")))
  }
}
