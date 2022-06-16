package org.wabase

import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers


class I18nSpecs extends FlatSpec with Matchers {
  import org.wabase.TestApp._
  implicit val locale = new java.util.Locale("lv")
  translate("""Field "%1$s" is not valid e-mail address""", "epasts") should be ("""Lauka "epasts" vērtība neatbilst e-pasta adreses formātam""")
  translate("to") should be ("līdz")
  translate("""Field "%1$s" is not valid e-mail address""", "epasts") should not be ("""Lauka "epasts" vērtība neatbilst e-pasta adreses šablonam""")
}
