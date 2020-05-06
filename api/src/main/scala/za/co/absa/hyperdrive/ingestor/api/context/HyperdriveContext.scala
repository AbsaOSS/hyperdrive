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

package za.co.absa.hyperdrive.ingestor.api.context

import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}
import scala.util.Try

object HyperdriveContext {
  private var contextValues = Map[String, Any]()

  def put(key: String, value: Any): Unit = {
    contextValues = contextValues + (key -> value)
  }

  def get[T: ClassTag : ru.TypeTag](key: String): Try[T] = {
    Try(
      contextValues(key) match {
        case asT: T => asT
        case value => throw new ClassCastException(s"Could not cast value $value for key $key to type ${ru.typeOf[T]}")
      }
    )
  }
}
