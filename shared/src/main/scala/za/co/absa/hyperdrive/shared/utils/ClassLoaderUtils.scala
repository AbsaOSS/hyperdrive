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

package za.co.absa.hyperdrive.shared.utils

import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}
import scala.util.{Failure, Success, Try}

object ClassLoaderUtils {

  def loadSingletonClassOfType[T:ClassTag:ru.TypeTag](fullyQualifiedName: String): T = {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    Try(mirror.staticModule(fullyQualifiedName)) match {
      case Success(module) => val reflectiveMirror = mirror.reflectModule(module)
        Try(reflectiveMirror.instance) match {
          case Success(instance) =>
            instance match {
              case singleton: T => singleton
              case _ => throw new IllegalArgumentException(s"Class '$fullyQualifiedName' is not an instance of '${ru.typeOf[T]}'")
            }
          case Failure(exception) => throw new IllegalArgumentException(s"Class '$fullyQualifiedName' is not a singleton", exception)
        }
      case Failure(exception) => throw new IllegalArgumentException(s"Class '$fullyQualifiedName' could not be found", exception)
    }
  }

}
