package org.wabase

import java.sql.SQLException

import org.mojoz.metadata.ViewDef.ViewDefBase
import org.mojoz.metadata.in.YamlMd
import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers
import org.tresql.ChildSaveException
import org.wabase.DefaultAppQuerease.ViewDef

import scala.collection.immutable.{Map, Seq}

class PostgreSqlConstraintMessageSpec extends FlatSpec with Matchers {
  behavior of "PostgreSqlConstraintMessage"

  object ConstraintTestApp extends AppBase[TestUsr] with NoAudit[TestUsr] with PostgresDbAccess with PostgreSqlConstraintMessage with NoAuthorization[TestUsr] with NoValidation {
    trait TestQuerease extends AppQuerease {
      override lazy val yamlMetadata = YamlMd.fromResource("/constraint-message-spec.yaml")
    }
    object TestQuerease extends TestQuerease

    override type QE = TestQuerease
    override protected def initQuerease: QE = TestQuerease
  }

  def error(code: String, message: String) = new SQLException(message, code)
  def childError(table: String, code: String, message: String) = new ChildSaveException(table, error(code, message))
  def getMessage(code: String, message: String, locale: String, viewName: String = "view1"): String =
    getMessageFromException(error(code, message), locale, viewName)

  def getMessageFromException(e: Exception, locale: String, viewName: String = "view1"): String = {
    try{
      val viewDef = ConstraintTestApp.qe.viewDef(viewName)
      ConstraintTestApp.friendlyConstraintErrorMessage(viewDef, {
        throw e
      })(new java.util.Locale(locale))
    }catch{
      case e: BusinessException => e.getMessage
    }
  }

  it should "handle not-null constraint message" in {
    val err = "ERROR: null value in column \"dokumenta_tips_id\" violates not-null constraint"
    getMessage("23502", err, "en") should be("Field \"dokumenta_tips_id\" must not be empty")
    getMessage("23502", err, "lv") should be("Lauks \"dokumenta_tips_id\" ir obligāts.")

    val someFieldErr = "ERROR: null value in column \"some_field\" violates not-null constraint"
    // find column in view fields
    getMessage("23502", someFieldErr, "en", "view1" ) should be("Field \"Field name in viewDef\" must not be empty")
    getMessage("23502", someFieldErr, "lv", "view1") should be("Lauks \"Field name in viewDef\" ir obligāts.")

    // find column in table def
    getMessage("23502", someFieldErr, "en", "view2" ) should be("Field \"Field name in tableDef\" must not be empty")
    getMessage("23502", someFieldErr, "lv", "view2") should be("Lauks \"Field name in tableDef\" ir obligāts.")
  }

  it should "handle not-null constraint message in child" in {
    val err = "ERROR: null value in column \"some_field\" violates not-null constraint"
    val childErr = childError("child_table", "23502", err)

    getMessageFromException(childErr, "en", "view_with_childs") should be("Field \"Field name in child\" must not be empty")
    getMessageFromException(childErr, "lv", "view_with_childs") should be("Lauks \"Field name in child\" ir obligāts.")
  }

  it should "handle fk error on delete" in {
    val err = "ERROR: update or delete on table \"kla_kodifikatora_ieraksts\" violates foreign key constraint \"fk_kla_kodifikatora_ieraksts_parent_id\" on table \"kla_kodifikatora_ieraksts\""
    getMessage("23503", err, "en") should be("Unable to find related entity (link fk_kla_kodifikatora_ieraksts_parent_id)")
    getMessage("23503", err, "lv") should be("Saistītais ieraksts (fk_kla_kodifikatora_ieraksts_parent_id) nav atrasts")

    // test constraint-translation.yaml
    val errCt = "ERROR: update or delete on table \"kla_kodifikatora_ieraksts\" violates foreign key constraint \"fk_kla_kodifikatora_ieraksts_parent_id_ct\" on table \"kla_kodifikatora_ieraksts\""
    getMessage("23503", errCt, "en") should be("Can't delete error message")
    getMessage("23503", errCt, "lv") should be("Can't delete error message")
  }

  it should "handle fk error on insert" in {
    val err = "ERROR: insert or update on table \"kla_kodifikatora_ieraksts\" violates foreign key constraint \"fk_kla_kodifikatora_ieraksts_parent_id\""
    getMessage("23503", err, "en") should be("Unable to find related entity (link fk_kla_kodifikatora_ieraksts_parent_id)")
    getMessage("23503", err, "lv") should be("Saistītais ieraksts (fk_kla_kodifikatora_ieraksts_parent_id) nav atrasts")

    // test constraint-translation.yaml
    val errCt = "ERROR: insert or update on table \"kla_kodifikatora_ieraksts\" violates foreign key constraint \"fk_kla_kodifikatora_ieraksts_parent_id_ct\""
    getMessage("23503", errCt, "en") should be("Can't find error message")
    getMessage("23503", errCt, "lv") should be("Can't find error message")
  }

  it should "handle unique constraint error" in {
    val err = "ERROR: duplicate key value violates unique constraint \"uk_persona_personas_kods\""
    getMessage("23505", err, "en") should be("Value should be unique (constraint uk_persona_personas_kods violated)")
    getMessage("23505", err, "lv") should be("Vērtībai jābūt unikālai (uk_persona_personas_kods)")

    // test constraint-translation.yaml
    val errCt = "ERROR: duplicate key value violates unique constraint \"uk_persona_personas_kods_ct\""
    getMessage("23505", errCt, "en") should be("Unique contraint violation error message")
    getMessage("23505", errCt, "lv") should be("Unique contraint violation error message")
  }

  it should "handle check constraint error" in {
    val err = "ERROR:  new row for relation \"ws_user\" violates check constraint \"department_check\""
    getMessage("23514", err, "en") should be("Invalid data (constraint department_check violated)")
    getMessage("23514", err, "lv") should be("Datu pārbaudes kļūda (department_check)")

    // test constraint-translation.yaml
    val errCt = "ERROR:  new row for relation \"ws_user\" violates check constraint \"department_check_ct\""
    getMessage("23514", errCt, "en") should be("Contraint violation error message")
    getMessage("23514", errCt, "lv") should be("Contraint violation error message")
  }
}
