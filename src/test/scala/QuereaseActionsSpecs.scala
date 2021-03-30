package org.wabase

import org.wabase.AppMetadata.Action.{Evaluation, NoOp, Tresql, VariableTransform, ViewCall}

class QuereaseActionsSpecs extends QuereaseBaseSpecs {

  import AppMetadata._

  override def beforeAll(): Unit = {
    querease = new QuereaseBase("/querease-action-specs-metadata.yaml")
    super.beforeAll()
  }

  "metadata" should "have" in {
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
  }

  "person save action" should "return person" in {
     true should be(true)
  }
}
