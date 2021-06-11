package org.wabase

import org.wabase.AppMetadata.Action.{Evaluation, NoOp, Tresql, VariableTransform, ViewCall}

class QuereaseActionsSpecs extends QuereaseBaseSpecs {

  import AppMetadata._

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
      "Person" -> classOf[Person],
      "PersonAccounts" -> classOf[PersonAccounts],
      "Person1" -> classOf[Person1],
      "Payment" -> classOf[Payment],
      "PersonList" -> classOf[PersonList],
      "PersonWithMainAccount" -> classOf[PersonWithMainAccount]
    )
  }


  override def beforeAll(): Unit = {
    querease = new QuereaseBase("/querease-action-specs-metadata.yaml") {
      override lazy val viewNameToClassMap = Dtos.viewNameToClass
    }
    super.beforeAll()
  }

  behavior of "metadata"

  it should "have" in {
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

  it should "fail account count validation" in {
    val p = new Person
    val pa = List(new PersonAccounts, new PersonAccounts, new PersonAccounts, new PersonAccounts)
    true should be(true)
  }

  it should "fail balance validation" in {
    true should be(true)
  }

  it should "return person" in {
     true should be(true)
  }
}

class QuereaseActionTestManager {
  def personSaveBizMethod(data: Map[String, Any]) = {
    data + ("name" -> s"Mr. ${data("name")}")
  }
}
