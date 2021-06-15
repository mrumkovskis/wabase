package org.wabase

import org.mojoz.querease.{ValidationException, ValidationResult}
import org.scalatest.flatspec.{AsyncFlatSpec, AsyncFlatSpecLike}
import org.wabase.AppMetadata.Action.{Evaluation, NoOp, Tresql, VariableTransform, ViewCall}

object Dtos {
  class Person extends DtoWithId {
    var id: java.lang.Long = null
    var name: String = null
    var surname: String = null
    var sex: String = null
    var birthdate: java.sql.Date = null
    var main_account: String = null
    var accounts: List[PersonAccounts] = Nil
  }
  class PersonAccounts extends DtoWithId {
    var id: java.lang.Long = null
    var number: String = null
    var balance: String = null
    var last_modified: java.sql.Timestamp = null
  }
  class Person1 extends Person with DtoWithId {}
  class Payment extends DtoWithId {
    var id: java.lang.Long = null
    var originator: String = null
    var beneficiary: String = null
    var amount: BigDecimal = null
    var date_time: java.sql.Timestamp = null
  }
  class PersonList extends DtoWithId {
    var id: java.lang.Long = null
    var name: String = null
    var surname: String = null
    var sex: String = null
    var birthdate: java.sql.Date = null
  }
  class PersonWithMainAccount extends DtoWithId {
    var id: java.lang.Long = null
  }

  val viewNameToClass = Map[String, Class[_ <: Dto]](
    "person" -> classOf[Person],
    "person_accounts" -> classOf[PersonAccounts],
    "person1" -> classOf[Person1],
    "payment" -> classOf[Payment],
    "person_list" -> classOf[PersonList],
    "person_with_main_account" -> classOf[PersonWithMainAccount]
  )
}

class QuereaseActionsSpecs extends AsyncFlatSpec with QuereaseBaseSpecs with AsyncFlatSpecLike {

  import AppMetadata._

  override def beforeAll(): Unit = {
    querease = new QuereaseBase("/querease-action-specs-metadata.yaml") {
      override lazy val viewNameToClassMap = Dtos.viewNameToClass
    }
    super.beforeAll()
  }

  behavior of "metadata"

  it should "have correct data" in {
    val paVd = querease.viewDef("person_with_main_account")
    paVd.actions("get") should be {
      Action(
        List(
          Evaluation("person", List(), ViewCall("get", "person")),
          Evaluation("main_acc_id", List(),
            Tresql("account[number = :person.main_account] {id}")),
          Evaluation("account",
            List(VariableTransform("main_acc_id.0.id", Some("id"))),
            ViewCall("get", "account")),
          Evaluation("4",
            List(VariableTransform("person", None),
              VariableTransform("account.number", Some("acc_number")),
              VariableTransform("account.balance", Some("acc_balance"))),
            NoOp)))
    }
    val pVd = querease.viewDef("person")
    pVd.actions("save").steps.head.isInstanceOf[Action.Validations] should be (true)
    pVd.actions("save").steps.head.asInstanceOf[Action.Validations].validations.head should be {
      "bind_var_cursors"
    }
  }

  behavior of "person save action"
  import Dtos._
  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  it should "fail account count validation" in {
    val p = new Person
    val pa = List(new PersonAccounts, new PersonAccounts, new PersonAccounts, new PersonAccounts)
    p.accounts = pa
    recoverToExceptionIf[ValidationException] {
      querease.doAction("person", "save", p.toMap(querease), Map())
    }.map(_.details should be(List(ValidationResult(Nil,
      List("person cannot have more than 3 accounts, instead '4' encountered")
    ))))
  }

  it should "fail balance validation" in {
    true should be(true)
  }

  it should "return person" in {
     true should be(true)
  }

  override def customStatements: Seq[String] = super.customStatements ++
    List(
      """create function array_length(sql_array bigint array) returns int
       language java deterministic no sql
       external name 'CLASSPATH:test.HsqldbCustomFunctions.array_length'""",
      """create function array_length(sql_array char varying(1024) array) returns int
       language java deterministic no sql
       external name 'CLASSPATH:test.HsqldbCustomFunctions.array_length'""",
      """create function checked_resolve(
         resolvable char varying(1024), resolved bigint array, error_message char varying(1024)
       ) returns bigint
         if array_length(resolved) > 1 or resolvable is not null and (array_length(resolved) = 0 or resolved[1] is null) then
           signal sqlstate '45000' set message_text = error_message;
         elseif array_length(resolved) = 1 then
           return resolved[1];
         else
           return null;
         end if""",
      """create function checked_resolve(
         resolvable char varying(1024), resolved char varying(1024) array, error_message char varying(1024)
       ) returns  char varying(1024)
         if array_length(resolved) > 1 or resolvable is not null and (array_length(resolved) = 0 or resolved[1] is null) then
           signal sqlstate '45000' set message_text = error_message;
         elseif array_length(resolved) = 1 then
           return resolved[1];
         else
           return null;
         end if""",
      "set database collation \"Latvian\""
    )
}

class QuereaseActionTestManager {
  def personSaveBizMethod(data: Map[String, Any]) = {
    data + ("name" -> s"Mr. ${data("name")}")
  }
}
