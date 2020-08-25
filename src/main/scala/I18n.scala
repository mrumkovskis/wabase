package org.wabase

import java.util.{Locale, ResourceBundle}

//for scala 2.12 compatibility
import scala.collection.convert.ImplicitConversions.`enumeration AsScalaIterator`
import scala.util.Try

trait I18n {

  val WabaseResourceName = "wabase"

  /** Subclass can override this value */
  val ResourceName = WabaseResourceName

  def bundle(name: String)(implicit locale: Locale): ResourceBundle = ResourceBundle.getBundle(name, locale)

  def translate(str: String, params: String*)(implicit locale: Locale): String = {
    translateFromBundle(ResourceName, str, params: _*)
  }

  def translateFromBundle(name: String, str: String, params: String*)(implicit locale: Locale): String = {
    Try(bundle(name).getString(str))
      .recover { case _ => str }
      .map(s => Try(s.format(params: _*)).getOrElse(s))
      .getOrElse(str)
  }

  /** Calls {{{i18nResourcesFromBundle(ResourceName)}}} */
  def i18nResources(implicit locale: Locale): Iterator[(String, Any)] = {
    i18nResourcesFromBundle(ResourceName)
  }

  /** Returns resources as {{{Iterator[(String, Any)]}}}. Iterator element is {{{(String, Any)}}} instead
    * of {{{(String, String)}}} so that {{{TupleJsonFormat}}} can be used for response marshalling
    * */
  def i18nResourcesFromBundle(name: String)(implicit locale: Locale): Iterator[(String, Any)] = {
    val b = bundle(name)
    b.getKeys.map(s => s -> Try(b.getString(s)).getOrElse(s))
  }
}
