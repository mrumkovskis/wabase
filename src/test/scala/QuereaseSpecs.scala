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

  class sys_user_role_ref_only_save extends DtoWithId {
    var id: java.lang.Long = null
    var user: sys_user_role_ref_only_save_user = null
    var role: sys_user_role_ref_only_save_role = null
  }
  class sys_user_role_ref_only_save_user extends DtoWithId {
    var id: java.lang.Long = null
    var person_id: java.lang.Long = null
  }
  class sys_user_role_ref_only_save_role extends Dto {
    var name: String = null
  }

  class sys_user_with_ro_roles extends DtoWithId {
    var id: java.lang.Long = null
    var name: String = null
    var roles: List[sys_user_role_choice] = null
  }

  class sys_user_with_roles                extends sys_user_with_ro_roles
  class sys_user_with_roles_save_on_insert extends sys_user_with_ro_roles
  class sys_user_with_roles_save_on_update extends sys_user_with_ro_roles
  class sys_user_with_roles_save_on_insert_legacy extends sys_user_with_ro_roles
  class sys_user_with_roles_save_on_update_legacy extends sys_user_with_ro_roles


  val viewNameToClass = Map[String, Class[_ <: Dto]](
    "person" -> classOf[Person],
    "sys_user_role_ref_only_save"      -> classOf[sys_user_role_ref_only_save],
    "sys_user_role_ref_only_save_role" -> classOf[sys_user_role_ref_only_save_role],
    "sys_user_role_ref_only_save_user" -> classOf[sys_user_role_ref_only_save_user],
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

    // save on insert only - legacy yaml syntax
    val u_lgi    = new sys_user_with_roles_save_on_insert_legacy
    u_lgi.name   = "user_lgi"
    u_lgi.roles  = List(role_a, role_d)
    val u_lgi_id = querease.save(u_lgi)
    u_lgi.id     = u_lgi_id
    querease.get[sys_user_with_roles_save_on_insert_legacy](u_lgi_id).get.name  shouldBe "user_lgi"
    querease.get[sys_user_with_roles_save_on_insert_legacy](u_lgi_id).get.roles.map(_.sys_role) shouldBe all_roles
    u_lgi.roles  = List(role_a)
    querease.save(u_lgi)
    querease.get[sys_user_with_roles_save_on_insert_legacy](u_lgi_id).get.roles.map(_.sys_role) shouldBe all_roles

    // save on update only - legacy yaml syntax
    val u_lgu    = new sys_user_with_roles_save_on_update_legacy
    u_lgu.name   = "user_lgu"
    u_lgu.roles  = List(role_a, role_d)
    val u_lgu_id = querease.save(u_lgu)
    u_lgu.id     = u_lgu_id
    querease.get[sys_user_with_roles_save_on_update_legacy](u_lgu_id).get.name  shouldBe "user_lgu"
    querease.get[sys_user_with_roles_save_on_update_legacy](u_lgu_id).get.roles.map(_.sys_role) shouldBe Nil
    querease.save(u_lgu)
    querease.get[sys_user_with_roles_save_on_update_legacy](u_lgu_id).get.roles.map(_.sys_role) shouldBe all_roles
    u_lgu.roles  = List(role_a)
    querease.save(u_lgu)
    querease.get[sys_user_with_roles_save_on_update_legacy](u_lgu_id).get.roles.map(_.sys_role) shouldBe List("admin")
    u_lgu.roles  = List(role_a, role_d)
    querease.save(u_lgu)
    querease.get[sys_user_with_roles_save_on_update_legacy](u_lgu_id).get.roles.map(_.sys_role) shouldBe all_roles

    // save refs only, save ref to user only on insert
    var refs_1  = new sys_user_role_ref_only_save
    refs_1.user = new sys_user_role_ref_only_save_user
    refs_1.role = new sys_user_role_ref_only_save_role

    refs_1.user.id    = -1
    refs_1.role.name  = role_a.sys_role
    intercept[org.tresql.TresqlException] {
      querease.save(refs_1)
    }.getMessage should include ("""Failed to identify value of "user"""")

    refs_1.user.id    = Query("sys_user u/person p[p.name = 'user_rwr'] {u.id}").unique[Long]
    refs_1.id = querease.save(refs_1)

    refs_1 = querease.get[sys_user_role_ref_only_save](refs_1.id).get
    refs_1.user.id        shouldBe Query("sys_user u/person p[p.name = 'user_rwr'] {u.id}").unique[Long]
    refs_1.user.person_id shouldBe Query("sys_user u/person p[p.name = 'user_rwr'] {u.person_id}").unique[Long]
    refs_1.role.name      shouldBe role_a.sys_role

    refs_1.user.id        = -1
    refs_1.user.person_id = -1
    refs_1.role.name      = role_d.sys_role
    querease.save(refs_1)

    refs_1 = querease.get[sys_user_role_ref_only_save](refs_1.id).get
    refs_1.user.id        shouldBe Query("sys_user u/person p[p.name = 'user_rwr'] {u.id}").unique[Long]
    refs_1.user.person_id shouldBe Query("sys_user u/person p[p.name = 'user_rwr'] {u.person_id}").unique[Long]
    refs_1.role.name      shouldBe role_d.sys_role

    refs_1.role.name      = "missing-r"
    intercept[org.tresql.TresqlException] {
      querease.save(refs_1)
    }.getCause.getMessage shouldBe """Failed to identify value of "role" (from sys_user_role_ref_only_save) - missing-r"""
  }
}
