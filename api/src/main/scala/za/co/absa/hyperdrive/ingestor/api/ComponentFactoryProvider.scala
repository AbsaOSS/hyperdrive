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

package za.co.absa.hyperdrive.ingestor.api

/**
 * Implementations of this trait are responsible for providing an instance of a [[ComponentFactory]] via the
 * service provider interface (SPI) and need to be registered in a provider configuration file under META-INF/services
 * Implementations must have a no-args constructor to be instantiable.
 */
trait ComponentFactoryProvider[T <: ComponentFactory[_]] {
  def getComponentFactory: T
}
