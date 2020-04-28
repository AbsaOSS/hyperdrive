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

import scala.collection.{mutable => mu}
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}
import scala.reflect.runtime.{universe => ru}

object HyperdriveContext {
  private val contextValues = mu.Map[String, Any]()

  def put(key: String, value: Any): Option[Any] = contextValues.put(key, value)

  def get[T:ClassTag:ru.TypeTag](key: String): Try[T] = {
    val value: Try[Any] = contextValues.get(key)
      .map(Success(_))
      .getOrElse(Failure(new NoSuchElementException(s"Could not find value for key $key")))

    value.flatMap {
      case asT: T => Success(asT)
      case _ => Failure(new ClassCastException(s"Could not cast value $value for key $key to type ${ru.typeOf[T]}"))
    }
  }
}
