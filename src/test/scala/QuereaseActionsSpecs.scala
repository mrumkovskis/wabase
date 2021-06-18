package org.wabase

import org.mojoz.querease.{ValidationException, ValidationResult}
import org.scalatest.flatspec.{AsyncFlatSpec, AsyncFlatSpecLike}
import org.tresql.Query
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
    var balance: BigDecimal = null
    var last_modified: java.sql.Timestamp = null
  }
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
          Evaluation(Some("person"), List(), ViewCall("get", "person")),
          Evaluation(Some("main_acc_id"), List(),
            Tresql("account[number = :person.main_account] {id}")),
          Evaluation(Some("account"),
            List(VariableTransform("main_acc_id.0.id", Some("id"))),
            ViewCall("get", "account")),
          Evaluation(None,
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
    p.accounts = Nil
    recoverToExceptionIf[ValidationException] {
      querease.doAction("person", "save", p.toMap(querease), Map())
    }.map(_.details should be(List(ValidationResult(Nil,
      List("person must have at least one account")
    ))))
  }

  it should "fail balance validation" in {
    val p = new Person
    val pa = new PersonAccounts
    pa.number = "AAA"
    pa.balance = 10
    p.accounts = List(new PersonAccounts, new PersonAccounts, pa)
    recoverToExceptionIf[ValidationException] {
      querease.doAction("person", "save", p.toMap(querease), Map())
    }.map(_.details should be(List(ValidationResult(Nil,
      List("Wrong balance for accounts 'AAA(10 != 0.00)'")
    ))))
    val pa1 = new PersonAccounts
    pa1.number = "BBB"
    pa1.balance = 2
    p.accounts = List(new PersonAccounts, pa, pa1)
    recoverToExceptionIf[ValidationException] {
      querease.doAction("person", "save", p.toMap(querease), Map())
    }.map(_.details should be(List(ValidationResult(Nil,
      List("Wrong balance for accounts 'AAA(10 != 0.00),BBB(2 != 0.00)'")
    ))))
  }

  it should "return person" in {
    val p = new Person
    p.name = "Kalis"
    p.surname = "Calis"
    p.sex = "M"
    p.birthdate = java.sql.Date.valueOf("1980-12-14")
    val pa = new PersonAccounts
    pa.number = "AAA"
    pa.balance = 0
    pa.last_modified = java.sql.Timestamp.valueOf("2021-06-17 17:16:00")
    p.accounts = List(pa)
    querease.doAction("person", "save", p.toMap(querease), Map()).map {
      case OptionResult(Some(pers)) => removeIds(pers.toMap(querease)) should be {
        Map("name" -> "Mr. Kalis", "surname" -> "Calis", "sex" -> "M",
          "birthdate" -> java.sql.Date.valueOf("1980-12-14"), "main_account" -> null, "accounts" ->
            List(Map("number" -> "AAA", "balance" -> 0.00,
              "last_modified" -> java.sql.Timestamp.valueOf("2021-06-17 17:16:00.0"))))
      }
    }

    p.name = "Zina"
    p.surname = "Mina"
    p.sex = "F"
    p.birthdate = java.sql.Date.valueOf("1982-12-14")
    pa.number = "BBB"
    pa.balance = 0
    pa.last_modified = java.sql.Timestamp.valueOf("2021-06-19 00:15:00")
    p.accounts = List(pa)
    querease.doAction("person", "save", p.toMap(querease), Map()).map {
      case OptionResult(Some(pers)) => removeIds(pers.toMap(querease)) should be {
        Map("main_account" -> null, "name" -> "Ms. Zina", "surname" -> "Mina", "sex" -> "F",
          "birthdate" -> java.sql.Date.valueOf("1982-12-14"), "accounts" ->
            List(Map("number" -> "BBB", "balance" -> 0.00,
              "last_modified" -> java.sql.Timestamp.valueOf("2021-06-19 00:15:00.0"))))
      }
    }
  }

  behavior of "payment save action"

  it should "fail amount validation" in {
    val p = new Payment
    p.amount = 0
    p.beneficiary = "AAA"
    recoverToExceptionIf[ValidationException] {
      querease.doAction("payment", "save", p.toMap(querease), Map())
    }.map(_.details should be(List(ValidationResult(Nil,
      List("Wrong amount 0. Amount must be greater than 0")
    ))))

    p.originator = "BBB"
    p.amount = 10
    recoverToExceptionIf[ValidationException] {
      querease.doAction("payment", "save", p.toMap(querease), Map())
    }.map(_.details should be(List(ValidationResult(Nil,
      List("Insufficient funds for account 'BBB'")
    ))))
  }

  it should "register payments" in {
    val p = new Payment
    p.amount = 10
    p.beneficiary = "AAA"
    querease.doAction("payment", "save", p.toMap(querease), Map()).map { res =>
      res.getClass.getName should be ("org.wabase.TresqlResult")
    }.flatMap { _ =>
      p.originator = "AAA"
      p.beneficiary = "BBB"
      p.amount = 2
      querease.doAction("payment", "save", p.toMap(querease), Map()).map { res =>
        res.getClass.getName should be ("org.wabase.TresqlResult")
      }
    }.map { _ =>
      Query("account{number, balance}#(1)").toListOfMaps should be(
        List(Map("number" -> "AAA", "balance" -> 8.00), Map("number" -> "BBB", "balance" -> 2.00)))
    }
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
    val address = if (data("sex") == "M") "Mr." else "Ms."
    data + ("name" -> s"$address ${data("name")}")
  }
}
