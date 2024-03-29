package org.wabase

import java.util.{Collections, Locale, PropertyResourceBundle, ResourceBundle}
import scala.jdk.CollectionConverters._
import scala.util.Try

case class I18Bundle(bundle: Iterator[(String, String)])

trait I18n {

  val I18nWabaseResourceName = "wabase"

  /** Application default resource bundle. Subclass can override this value */
  val I18nResourceName = I18nWabaseResourceName

  /** Application resource bundle dependencies. */
  def i18nResourceDependencies: Map[String, String] = Map()

  private lazy val loaderControl =
    new ResourceBundle.Control {
      override def getFormats(baseName: String): java.util.List[String] =
        ResourceBundle.Control.FORMAT_PROPERTIES
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
          def getInputStream(baseName: String) = {
            def getStream(locale: Locale) = {
              val bundleName: String = toBundleName(baseName, locale)
              val resourceName: String = toResourceName(bundleName, "properties")
              if (reload) {
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
            }
            val stream = getStream(locale)
            if  (stream == null && baseName == I18nWabaseResourceName)
                 getStream(Locale.ENGLISH) // default to en for wabase built-in messages
            else stream
          }
          @annotation.tailrec
          def resourceNameChain(baseName: String, chain: List[String] = Nil): List[String] = {
            val fallbackName = i18nResourceDependencies.getOrElse(baseName, I18nWabaseResourceName)
            if  (baseName == fallbackName)
                 baseName :: chain
            else resourceNameChain(fallbackName, baseName :: chain)
          }
          val streams =
            resourceNameChain(baseName)
            .map(getInputStream)
            .filter(_ != null)
          val stream =
            if  (streams.isEmpty) null
            else new java.io.SequenceInputStream(Collections.enumeration(streams.asJava))
          if (stream != null) {
            val br = new BufferedReader(new InputStreamReader(stream, "UTF-8"))
            val bundle: ResourceBundle = new PropertyResourceBundle(br)
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
  def i18nResources(implicit locale: Locale): I18Bundle = {
    i18nResourcesFromBundle(I18nResourceName)
  }

  /**
    * Returns resources as {{{I18Bundle}}}.
    * */
  def i18nResourcesFromBundle(name: String)(implicit locale: Locale): I18Bundle = {
    val b = bundle(name)
    I18Bundle(b.getKeys.asScala.map(s => s -> Try(b.getString(s)).getOrElse(s)))
  }
}
