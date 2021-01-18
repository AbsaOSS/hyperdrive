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

package za.co.absa.hyperdrive.ingestor.implementation.mongodbutils

import java.util.concurrent.TimeUnit

import org.mongodb.scala.{FindObservable, Observable, Observer, SingleObservable}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Promise}

/**
  * These implicits extend observables returned by Scala MongoDB driver with .execute() method
  * to make synchronous calls.
  */
object ScalaMongoImplicits {
  private val executionTimeout = Duration(300, TimeUnit.SECONDS)

  implicit class ObservableExecutor[T](observable: Observable[T]) {
    def execute(): Seq[T] = Await.result(observable.toFuture, executionTimeout)
  }

  implicit class SingleObservableExecutor[T](observable: SingleObservable[T]) {
    def execute(): T = Await.result(observable.toFuture, executionTimeout)
  }

  implicit class FindObservableExecutor[T](observable: FindObservable[T]) {
    def execute(): Seq[T] = Await.result(observable.toFuture, executionTimeout)
  }

  implicit class FindObservableTraversable[T](observable: FindObservable[T]) {
    def syncForeach(transform: T => Unit): Unit = {
      val p = Promise[Unit]()
      val f = p.future

      observable.subscribe(new Observer[T] {
        override def onNext(result: T): Unit = transform(result)

        override def onError(e: Throwable): Unit = p.failure(new RuntimeException("Error fetching MongoDB documents.", e))

        override def onComplete(): Unit = p.success((): Unit)
      })

      // An infinite wait since processing all documents in a collection can take a long time
      Await.result(f, Duration.Inf)
    }
  }

}
