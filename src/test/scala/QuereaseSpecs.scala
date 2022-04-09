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

  class sys_user_with_roles                extends sys_user_with_ro_roles
  class sys_user_with_roles_save_on_insert extends sys_user_with_ro_roles
  class sys_user_with_roles_save_on_update extends sys_user_with_ro_roles

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
      Some(Filters(
        Some( "(:surname != 'Readonly')"),
        Some("(p.surname != 'Readonly')"),
        Some("(p.surname  = 'Readonly')"),
      )),
      "p",
      true,
      true,
      List(
        Property("id",TresqlValue(":id",true,true)),
        Property("name",TresqlValue(":name",true,true)),
        Property("surname",TresqlValue(":surname",true,true)),
      ),
      null
    )
    querease.persistenceMetadata("sys_user_with_ro_roles") shouldBe View(
      List(
        SaveTo("person",Set(),List()),
        SaveTo("sys_user",Set("person_id", "id"),List())
      ),
      Some(Filters(None,None,None)),
      "u",
      true,
      true,
      List(
        Property("id",TresqlValue(":id",true,true)),
        Property("name",TresqlValue(":name",true,true)),
      ),
      null
    )
    querease.persistenceMetadata("sys_user_with_roles") shouldBe View(
      List(
        SaveTo("person",Set(),List()),
        SaveTo("sys_user",Set("person_id", "id"),List())
      ),
      Some(Filters(None,None,None)),
      "u",
      true,
      true,
      List(
        Property("id",TresqlValue(":id",true,true)),
        Property("name",TresqlValue(":name",true,true)),
        Property("roles",ViewValue(
          View(
            List(SaveTo("sys_user_role",Set(),List())),
            Some(Filters(None,None,None)),
            "ur",
            true,
            true,
            List(
              Property("id",TresqlValue(":id",true,true)),
              Property("sys_role_id",TresqlValue(
                """(checked_resolve(:sys_role, array(sys_role r[name = :sys_role]{r.id}@(2)),""" +
                """ 'Failed to identify value of "sys_role" (from sys_user_role_choice) - '""" +
                """ || coalesce(:sys_role, 'null')))""",true,true)),
            ),
            null
          ),
          SaveOptions(true,false,true),
        ))
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
    }.getMessage shouldBe "Record not inserted into table(s): person"

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
    }.getMessage shouldBe "Record not updated in table(s): person"

    p = querease.get[Person](id).get
    p.id shouldBe id
    p.name shouldBe "Name"
    p.surname shouldBe "Readonly"

    querease.delete(p) shouldBe 1
    querease.get[Person](id) shouldBe None
  }

  it should "respect field options" in {
    val all_roles = List("admin", "demo")
    all_roles.foreach { role =>
      Query(s"+sys_role {id, name} [#sys_role, '$role']")
    }

    val role_a = new sys_user_role_choice
    role_a.sys_role = "admin"

    val role_d = new sys_user_role_choice
    role_d.sys_role = "demo"

    // read-only children - never save
    val u_ror    = new sys_user_with_ro_roles
    u_ror.name   = "user_ror"
    u_ror.roles  = List(role_a, role_d)
    val u_ror_id = querease.save(u_ror)
    u_ror.id     = u_ror_id
    querease.get[sys_user_with_ro_roles](u_ror_id).get.name  shouldBe "user_ror"
    querease.get[sys_user_with_ro_roles](u_ror_id).get.roles shouldBe Nil
    querease.save(u_ror)
    querease.get[sys_user_with_ro_roles](u_ror_id).get.roles shouldBe Nil

    // read-write children - always save
    val u_rwr    = new sys_user_with_roles
    u_rwr.name   = "user_rwr"
    u_rwr.roles  = List(role_a, role_d)
    val u_rwr_id = querease.save(u_rwr)
    u_rwr.id     = u_rwr_id
    querease.get[sys_user_with_roles](u_rwr_id).get.name  shouldBe "user_rwr"
    querease.get[sys_user_with_roles](u_rwr_id).get.roles.map(_.sys_role) shouldBe all_roles
    u_rwr.roles  = List(role_a)
    querease.save(u_rwr)
    querease.get[sys_user_with_roles](u_rwr_id).get.roles.map(_.sys_role) shouldBe List("admin")
    u_rwr.roles  = List(role_a, role_d)
    querease.save(u_rwr)
    querease.get[sys_user_with_roles](u_rwr_id).get.roles.map(_.sys_role) shouldBe all_roles

    // save on insert only
    val u_sir    = new sys_user_with_roles_save_on_insert
    u_sir.name   = "user_sir"
    u_sir.roles  = List(role_a, role_d)
    val u_sir_id = querease.save(u_sir)
    u_sir.id     = u_sir_id
    querease.get[sys_user_with_roles_save_on_insert](u_sir_id).get.name  shouldBe "user_sir"
    querease.get[sys_user_with_roles_save_on_insert](u_sir_id).get.roles.map(_.sys_role) shouldBe all_roles
    u_sir.roles  = List(role_a)
    querease.save(u_sir)
    querease.get[sys_user_with_roles_save_on_insert](u_sir_id).get.roles.map(_.sys_role) shouldBe all_roles

    // save on update only
    val u_sur    = new sys_user_with_roles_save_on_update
    u_sur.name   = "user_sur"
    u_sur.roles  = List(role_a, role_d)
    val u_sur_id = querease.save(u_sur)
    u_sur.id     = u_sur_id
    querease.get[sys_user_with_roles_save_on_update](u_sur_id).get.name  shouldBe "user_sur"
    querease.get[sys_user_with_roles_save_on_update](u_sur_id).get.roles.map(_.sys_role) shouldBe Nil
    querease.save(u_sur)
    querease.get[sys_user_with_roles_save_on_update](u_sur_id).get.roles.map(_.sys_role) shouldBe all_roles
    u_sur.roles  = List(role_a)
    querease.save(u_sur)
    querease.get[sys_user_with_roles_save_on_update](u_sur_id).get.roles.map(_.sys_role) shouldBe List("admin")
    u_sur.roles  = List(role_a, role_d)
    querease.save(u_sur)
    querease.get[sys_user_with_roles_save_on_update](u_sur_id).get.roles.map(_.sys_role) shouldBe all_roles
  }
}
