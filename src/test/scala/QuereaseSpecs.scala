package org.wabase

import org.mojoz.querease.{ValidationException, ValidationResult}
import org.scalatest.flatspec.{AsyncFlatSpec, AsyncFlatSpecLike}
import org.scalatest.matchers.should.Matchers
import org.tresql.{Query, Resources}
import org.wabase.QuereaseSpecsDtos.Person

import scala.concurrent.{ExecutionContext, Future}

object QuereaseSpecsDtos {
  class Person extends DtoWithId {
    var id: java.lang.Long = null
    var name: String = null
    var surname: String = null
  }

  class sys_user_role_choice extends DtoWithId {
    var id: java.lang.Long = null
    var sys_role: String = null
  }

  class sys_user_with_ro_roles extends DtoWithId {
    var id: java.lang.Long = null
    var name: String = null
    var roles: List[sys_user_role_choice] = null
  }

  class sys_user_with_roles extends sys_user_with_ro_roles

  val viewNameToClass = Map[String, Class[_ <: Dto]](
    "person" -> classOf[Person],
    "sys_user_role_choice" -> classOf[sys_user_role_choice],
    "sys_user_with_ro_roles" -> classOf[sys_user_with_ro_roles],
    "sys_user_with_roles" -> classOf[sys_user_with_roles],
  )
}

class QuereaseSpecs extends AsyncFlatSpec with Matchers with TestQuereaseInitializer with AsyncFlatSpecLike {

  import AppMetadata._

  implicit protected var tresqlResources: Resources = _

  override def beforeAll(): Unit = {
    querease = new TestQuerease("/querease-specs-metadata.yaml") {
      override lazy val viewNameToClassMap = QuereaseSpecsDtos.viewNameToClass
    }
    super.beforeAll()
    tresqlResources = tresqlThreadLocalResources.withConn(tresqlThreadLocalResources.conn)
  }

  import QuereaseSpecsDtos._
  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  "querease" should "build correct persistence metadata" in {
    import org.tresql.OrtMetadata._
    querease.persistenceMetadata("person") shouldBe View(
      List(SaveTo("person",Set(),List())),
      null,
      Some(Filters(
        Some( "(:surname != 'Readonly')"),
        Some("(p.surname != 'Readonly')"),
        Some("(p.surname  = 'Readonly')"),
      )),
      "p",
      List(
        Property("id",TresqlValue(":id",true,true)),
        Property("name",TresqlValue(":name",true,true)),
        Property("surname",TresqlValue(":surname",true,true)),
      ),
      null
    )
    querease.persistenceMetadata("sys_user_with_ro_roles") shouldBe View(
      List(SaveTo("sys_user",Set(),List())),
      null,
      Some(Filters(None,None,None)),
      "u",
      List(
        Property("id",TresqlValue(":id",true,true)),
      ),
      null
    )
    querease.persistenceMetadata("sys_user_with_roles") shouldBe View(
      List(SaveTo("sys_user",Set(),List())),
      null,
      Some(Filters(None,None,None)),
      "u",
      List(
        Property("id",TresqlValue(":id",true,true)),
        Property("roles",ViewValue(View(
          List(SaveTo("sys_user_role",Set(),List())),
          SaveOptions(true,false,false),
          Some(Filters(None,None,None)),
          "ur",
          List(
            Property("id",TresqlValue(":id",true,true)),
            Property("sys_role_id",TresqlValue(
              """(checked_resolve(:sys_role, array(sys_role r[name = :sys_role]{r.id}@(2)),""" +
              """ 'Failed to identify value of "sys_role" (from sys_user_role_choice) - '""" +
              """ || coalesce(:sys_role, 'null')))""",true,true)),
          ),
          null
        )))
      ),
      null
    )
  }

  it should "respect horizontal auth filters" in {
    var p = new Person
    p.name = "Name"
    p.surname = "Readonly"

    intercept[org.mojoz.querease.NotFoundException] {
      querease.save(p)
    }.getMessage shouldBe "Record not inserted into table(s): person p"

    p.surname = "Surname"
    val id = querease.save(p)

    p = querease.get[Person](id).get
    p.id shouldBe id
    p.name shouldBe "Name"
    p.surname shouldBe "Surname"

    intercept[org.mojoz.querease.NotFoundException] {
      querease.delete(p)
    }.getMessage shouldBe "Record not deleted in table person"

    p = querease.get[Person](id).get
    p.id shouldBe id
    p.name shouldBe "Name"
    p.surname shouldBe "Surname"
    p.surname = "Readonly"
    querease.save(p)

    p = querease.get[Person](id).get
    p.id shouldBe id
    p.name shouldBe "Name"
    p.surname shouldBe "Readonly"
    p.surname = "Surname"

    intercept[org.mojoz.querease.NotFoundException] {
      querease.save(p)
    }.getMessage shouldBe "Record not updated in table(s): person p"

    p = querease.get[Person](id).get
    p.id shouldBe id
    p.name shouldBe "Name"
    p.surname shouldBe "Readonly"

    querease.delete(p) shouldBe 1
    querease.get[Person](id) shouldBe None
  }
}
