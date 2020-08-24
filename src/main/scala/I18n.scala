package org.wabase

import java.util.{Locale, ResourceBundle}

//for scala 2.12 compatibility
import scala.collection.convert.ImplicitConversions.`enumeration AsScalaIterator`
import scala.util.Try

trait I18n {

  val WabaseResourceName = "wabase"

  def bundle(name: String, locale: Locale): ResourceBundle = ResourceBundle.getBundle(name, locale)
  def wabaseBundle(locale: Locale): ResourceBundle = bundle(WabaseResourceName, locale)

  def translate(name: String, locale: Locale, str: String, params: String*): String = {
    Try(bundle(name, locale).getString(str))
      .recover { case _ => str }
      .map(s => Try(s.format(params: _*)).getOrElse(s))
      .getOrElse(str)
  }

  def translate(locale: Locale, str: String, params: String*): String = {
    translate(WabaseResourceName, locale, str, params: _*)
  }

  /** Returns resources as {{{Iterator[(String, Any)]}}}. Iterator element is {{{(String, Any)}}} instead
    * of {{{(String, String)}}} so that {{{TupleJsonFormat}}} can be used for response marshalling
    * */
  def resources(name: String, locale: Locale): Iterator[(String, Any)] = {
    val b = bundle(name, locale)
    b.getKeys.map(s => s -> Try(b.getString(s)).getOrElse(s))
  }
}
