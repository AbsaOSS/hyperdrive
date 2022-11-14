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

package za.co.absa.hyperdrive.ingestor.implementation.testutils.mongodb

import org.mongodb.scala.{MongoClient, MongoDatabase}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait MongoDbFixture extends BeforeAndAfterAll {

  this: Suite =>

  // Workaround to fix version not supported error
  if (System.getProperty("os.arch") == "aarch64" && System.getProperty("os.name") == "Mac OS X") {
    System.setProperty("os.arch", "i686_64")
  }

  import ScalaMongoImplicits._

  private val (mongoDbExecutable, mongoPort) = EmbeddedMongoDbSingleton.embeddedMongoDb

  def uri: String = s"mongodb://localhost:$mongoPort"

  protected val dbName: String = "unit_test_database"

  protected var connection: MongoDbConnection = _
  protected var db: MongoDatabase = _

  private var mongoClient: MongoClient = _

  def clearDb(): Unit = {
    val dbs = mongoClient.listDatabaseNames().execute()
    if (dbs.contains(dbName)) {
      db.drop().execute()
    }

    db = mongoClient.getDatabase(dbName)
  }

  override protected def beforeAll(): Unit = {
    mongoClient = MongoClient(uri)

    connection = MongoDbConnection.getConnection(mongoClient, uri, dbName)

    val dbs = mongoClient.listDatabaseNames().execute()
    if (dbs.contains(dbName)) {
      throw new IllegalStateException(s"MongoDB unit test database " +
        s"'$dbName' already exists at '$dbName'.")
    }

    db = mongoClient.getDatabase(dbName)

    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    try super.afterAll()
    finally mongoClient.getDatabase(dbName).drop().execute()
  }
}
