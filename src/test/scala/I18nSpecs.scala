package org.wabase

import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers


class I18nSpecs extends FlatSpec with Matchers {
  import org.wabase.TestApp._
  val lv = new java.util.Locale("lv")
  val en = new java.util.Locale("en")
  val ja = new java.util.Locale("ja")

  translate("Record not found, cannot edit")  (lv) shouldBe "Ierakstu nevar labot, jo to neizdodas atrast"
  translate("Record not found, cannot delete")(lv) shouldBe "Ierakstu nevar dzēst, jo to neizdodas atrast"

  translate("Record not found, cannot edit")  (en) shouldBe "Record not found, cannot edit"
  translate("Record not found, cannot delete")(en) shouldBe "Record not found, cannot delete"

  // default to en for wabase built-in messages
  translate("Record not found, cannot edit")  (ja) shouldBe "Record not found, cannot edit"

  translate("""Field "%1$s" is not valid e-mail address""", "epasts")(en) shouldBe """Field "epasts" is not valid e-mail address"""

  implicit val locale = new java.util.Locale("lv")
  translate("""Field "%1$s" is not valid e-mail address""", "epasts") should be ("""Lauka "epasts" vērtība neatbilst e-pasta adreses formātam""")
  translate("to") should be ("līdz")
  translate("""Field "%1$s" is not valid e-mail address""", "epasts") should not be ("""Lauka "epasts" vērtība neatbilst e-pasta adreses šablonam""")
}
