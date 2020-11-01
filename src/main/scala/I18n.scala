package org.wabase

import java.util.{Locale, PropertyResourceBundle, ResourceBundle}

import scala.jdk.CollectionConverters._
import scala.util.Try

trait I18n {

  val I18nWabaseResourceName = "wabase"

  /** Application default resource bundle. Subclass can override this value */
  val I18nResourceName = I18nWabaseResourceName

  /** Application resource bundle dependencies. */
  def i18nResourceDependencies: Map[String, String] = Map()

  private lazy val loaderControl =
    new ResourceBundle.Control {
      override def newBundle(baseName: String,
                             locale: Locale,
                             format: String,
                             loader: ClassLoader,
                             reload: Boolean): ResourceBundle = {
        if (baseName == null || locale == null || format == null || loader == null)
          throw new NullPointerException()
        if (ResourceBundle.Control.FORMAT_PROPERTIES.contains(format)) {
          import java.io._
          import java.net._
          val bundleName: String = toBundleName(baseName, locale)
          val resourceName: String = toResourceName(bundleName, "properties")
          val stream = if (reload) {
            val url: URL = loader.getResource(resourceName)
            if (url != null) {
              val connection: URLConnection = url.openConnection()
              if (connection != null) {
                // Disable caches to get fresh data for
                // reloading.
                connection.setUseCaches(false)
                connection.getInputStream()
              } else null
            } else null
          } else {
            loader.getResourceAsStream(resourceName)
          }
          if (stream != null) {
            val br = new BufferedReader(new InputStreamReader(stream, "UTF-8"))
            val bundle: ResourceBundle =
              if (baseName == I18nWabaseResourceName) new PropertyResourceBundle(br) else {
                new PropertyResourceBundle(br) {
                  val parentBundle = i18nResourceDependencies.getOrElse(baseName, I18nWabaseResourceName)
                  setParent(I18n.this.bundle(parentBundle)(locale))
                }
              }
            br.close()
            bundle
          } else null
        } else {
          super.newBundle(baseName, locale, format, loader, reload)
        }
      }
    }

  def bundle(name: String)(implicit locale: Locale): ResourceBundle =
    ResourceBundle.getBundle(name, locale, loaderControl)

  def translate(str: String, params: String*)(implicit locale: Locale): String = {
    translateFromBundle(I18nResourceName, str, params: _*)
  }

  def translateFromBundle(name: String, str: String, params: String*)(implicit locale: Locale): String = {
    Try(bundle(name).getString(str))
      .recover { case _ => str }
      .map(s => Try(s.format(params: _*)).getOrElse(s))
      .getOrElse(str)
  }

  /** Calls {{{i18nResourcesFromBundle(ResourceName)}}} */
  def i18nResources(implicit locale: Locale): Iterator[(String, Any)] = {
    i18nResourcesFromBundle(I18nResourceName)
  }

  /** Returns resources as {{{Iterator[(String, Any)]}}}. Iterator element is {{{(String, Any)}}} instead
    * of {{{(String, String)}}} so that {{{TupleJsonFormat}}} can be used for response marshalling
    * */
  def i18nResourcesFromBundle(name: String)(implicit locale: Locale): Iterator[(String, Any)] = {
    val b = bundle(name)
    b.getKeys.asScala.map(s => s -> Try(b.getString(s)).getOrElse(s))
  }
}
