package org.wabase

import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers
import org.wabase.AppMetadata.DbAccessKey

class MultiPoolTest extends FlatSpec with Matchers with ScalatestRouteTest with BeforeAndAfterAll {
  behavior of "dbUse() and transaction()"

  implicit val queryTimeout = QueryTimeout(10)

  val db = new DbAccess with Loggable {
    override implicit val tresqlResources = new TresqlResources {
      override lazy val resourcesTemplate = super.resourcesTemplate
    }
  }

  import db._

  override def beforeAll() = {
    DbDrivers.loadDrivers
    transaction {
      val statement = db.tresqlResources.conn.createStatement
      statement execute """ALTER SCHEMA PUBLIC RENAME TO TEST1"""
      statement.close()
    }(implicitly[QueryTimeout], pool = PoolName("test1"), Nil)
    transaction {
      val statement = db.tresqlResources.conn.createStatement
      statement execute """ALTER SCHEMA PUBLIC RENAME TO TEST2"""
      statement.close()
    }(implicitly[QueryTimeout], pool = PoolName("test2"), Nil)
  }

  it should "work with default pool" in {
    implicit val pool = DEFAULT_CP
    implicit val extraDb: Seq[DbAccessKey] = Nil
    db.tresqlResources.conn should be(null)
    dbUse{
      db.tresqlResources.conn should not be(null)
    }
    db.tresqlResources.conn should be(null)
    transaction{
      db.tresqlResources.conn should not be(null)
    }
    db.tresqlResources.conn should be(null)
  }

  it should "work with implicit pool name" in {
    {
      implicit val pool = PoolName("test1")
      implicit val extraDb: Seq[DbAccessKey] = Nil
      db.tresqlResources.conn should be(null)
      dbUse{
        db.tresqlResources.conn should not be(null)
        db.tresqlResources.conn.getSchema should be("TEST1")
      }
      db.tresqlResources.conn should be(null)
      transaction{
        db.tresqlResources.conn should not be(null)
        db.tresqlResources.conn.getSchema should be("TEST1")
      }
      db.tresqlResources.conn should be(null)
    }
    {
      implicit val pool = PoolName("test2")
      implicit val extraDb: Seq[DbAccessKey] = Nil
      db.tresqlResources.conn should be(null)
      dbUse{
        db.tresqlResources.conn should not be(null)
        db.tresqlResources.conn.getSchema should be("TEST2")
      }
      db.tresqlResources.conn should be(null)
      transaction{
        db.tresqlResources.conn should not be(null)
        db.tresqlResources.conn.getSchema should be("TEST2")
      }
      db.tresqlResources.conn should be(null)
    }
  }

  it should "keep connection for nested calls" in {
    implicit val pool = PoolName("test1")
    implicit val extraDb: Seq[DbAccessKey] = Nil
    db.tresqlResources.conn should be(null)
    dbUse{
      val con = db.tresqlResources.conn
      dbUse{
        db.tresqlResources.conn should be(con)
      }
      transaction{
        db.tresqlResources.conn should be(con)
      }
    }
    transaction{
      val con = db.tresqlResources.conn
      dbUse{
        db.tresqlResources.conn should be(con)
      }
      transaction{
        db.tresqlResources.conn should be(con)
      }
    }
  }

  it should "open connection if nested call is from different pool" in {
    implicit val pool = PoolName("test1")
    implicit val extraDb: Seq[DbAccessKey] = Nil
    db.tresqlResources.conn should be(null)
    dbUse{
      db.tresqlResources.conn.getSchema should be("TEST1")

      dbUse{
        db.tresqlResources.conn.getSchema should be("TEST2")
      }(implicitly[QueryTimeout], pool = PoolName("test2"), extraDb = extraDb)

      db.tresqlResources.conn.getSchema should be("TEST1")

      transaction{
        db.tresqlResources.conn.getSchema should be("TEST2")
      }(implicitly[QueryTimeout], pool = PoolName("test2"), extraDb = extraDb)

      db.tresqlResources.conn.getSchema should be("TEST1")
    }
    db.tresqlResources.conn should be(null)

    transaction{
      db.tresqlResources.conn.getSchema should be("TEST1")

      dbUse{
        db.tresqlResources.conn.getSchema should be("TEST2")
      }(implicitly[QueryTimeout], pool = PoolName("test2"), extraDb = extraDb)

      db.tresqlResources.conn.getSchema should be("TEST1")

      transaction{
        db.tresqlResources.conn.getSchema should be("TEST2")
      }(implicitly[QueryTimeout], pool = PoolName("test2"), extraDb = extraDb)

      db.tresqlResources.conn.getSchema should be("TEST1")
    }
    db.tresqlResources.conn should be(null)

  }

  it should "handle multiple levels of nested calls" in {
    implicit val pool = DEFAULT_CP
    implicit val extraDb: Seq[DbAccessKey] = Nil
    db.tresqlResources.conn should be(null)

    transaction{
      db.tresqlResources.conn.getSchema should be("PUBLIC")

      dbUse{
        db.tresqlResources.conn.getSchema should be("TEST2")

        transaction{
          db.tresqlResources.conn.getSchema should be("TEST1")
        }(implicitly[QueryTimeout], pool = PoolName("test1"), extraDb = extraDb)

        db.tresqlResources.conn.getSchema should be("TEST2")
      }(implicitly[QueryTimeout], pool = PoolName("test2"), extraDb = extraDb)


      db.tresqlResources.conn.getSchema should be("PUBLIC")
    }

    db.tresqlResources.conn should be(null)

    transaction{
      db.tresqlResources.conn.getSchema should be("PUBLIC")
      val publicCon = db.tresqlResources.conn
      dbUse{
        db.tresqlResources.conn.getSchema should be("PUBLIC")
        db.tresqlResources.conn should be(publicCon)

        transaction{
          db.tresqlResources.conn.getSchema should be("TEST1")
        }(implicitly[QueryTimeout], pool = PoolName("test1"), extraDb = extraDb)

        db.tresqlResources.conn.getSchema should be("PUBLIC")
        db.tresqlResources.conn should be(publicCon)
      }

      db.tresqlResources.conn.getSchema should be("PUBLIC")
      db.tresqlResources.conn should be(publicCon)
    }

    db.tresqlResources.conn should be(null)
  }

  behavior of "transaction and transactionNew"

  it should "work with transaction in transaction" in {
    implicit val pool = DEFAULT_CP
    implicit val extraDb: Seq[DbAccessKey] = Nil
    transaction{
      val publicCon = db.tresqlResources.conn
      transaction{
        db.tresqlResources.conn should be(publicCon)
      }
    }
  }

  it should "work with transactionNew in transaction" in {
    implicit val pool = DEFAULT_CP
    implicit val extraDb: Seq[DbAccessKey] = Nil
    transaction {
      val publicCon = db.tresqlResources.conn
      transactionNew {
        db.tresqlResources.conn should not be (publicCon)
      }
    }
  }

  it should "work with transactionNew in transactionNew" in {
    implicit val pool = DEFAULT_CP
    implicit val extraDb: Seq[DbAccessKey] = Nil
    transactionNew {
      val publicCon = db.tresqlResources.conn
      transactionNew {
        db.tresqlResources.conn should not be (publicCon)
      }
    }
  }

  it should "work with transaction in transactionNew" in {
    implicit val pool = DEFAULT_CP
    implicit val extraDb: Seq[DbAccessKey] = Nil
    transactionNew {
      val publicCon = db.tresqlResources.conn
      transaction {
        db.tresqlResources.conn should be(publicCon)
      }
    }
  }

  it should "keep connected for nested dbUse" in {
    implicit val pool = DEFAULT_CP
    implicit val extraDb: Seq[DbAccessKey] = Nil
    transaction{
      val publicCon = db.tresqlResources.conn
      dbUse{
        db.tresqlResources.conn should be(publicCon)
      }
    }

    transactionNew{
      val publicCon = db.tresqlResources.conn
      dbUse{
        db.tresqlResources.conn should be(publicCon)
      }
    }
  }

  it should "use connection from dbUse when required" in {
    implicit val pool = DEFAULT_CP
    implicit val extraDb: Seq[DbAccessKey] = Nil
    dbUse {
      val publicCon = db.tresqlResources.conn
      transaction {
        db.tresqlResources.conn should be(publicCon)
      }
    }

    dbUse {
      val publicCon = db.tresqlResources.conn
      transactionNew {
        db.tresqlResources.conn should not be(publicCon)
      }
    }
  }
}
