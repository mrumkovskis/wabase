package org.wabase

import com.typesafe.config.Config
import io.bullet.borer.Json
import org.apache.pekko.http.scaladsl.marshalling.{Marshaller, ToEntityMarshaller}
import org.apache.pekko.http.scaladsl.model.MediaTypes.`application/json`
import org.apache.pekko.http.scaladsl.model.{HttpEntity, HttpRequest}
import org.apache.pekko.http.scaladsl.server.LanguageNegotiator

import java.util
import java.util.{ListResourceBundle, Locale, MissingResourceException, ResourceBundle}
import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.collection.mutable.ArrayBuffer
import org.snakeyaml.engine.v2.api.{Load, LoadSettings}
import org.tresql.Query
import org.wabase.ds.PoolName

import scala.annotation.tailrec

case class I18Bundle(bundle: Iterator[(String, String)])

trait I18n { this: WabaseApp[_] with DbAccess =>

  val I18nResourceName = "i18n"

  protected lazy val loaderControl =
    new I18n.ResourceBundleLoader(I18nResourceName, config.getConfig(I18nResourceName), this)

  def bundle(name: String)(implicit locale: Locale): ResourceBundle =
    ResourceBundle.getBundle(name, locale, loaderControl)

  /** Translates message template and formats it with parameters using locale. Parameters are passed to format
    * as is, so format specifiers other than %s (i.e. %d, %.2f) can be used. If formatting fails, translated
    * template is returned. */
  def translate(str: String, params: Any*)(implicit locale: Locale): String =
    translateFromBundle(I18nResourceName, str, params: _*)

  def translateFromBundle(name: String, str: String, params: Any*)(implicit locale: Locale): String = {
    Try(bundle(name).getString(str))
      .recover { case _: MissingResourceException => str }
      .map(s => Try(s.formatLocal(locale, params: _*)).getOrElse(s))
      .get
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

object I18n {
  /** Language, script, country(region) regexps are taken from
   * [[https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/Locale.html]]
   * */
  val LocaleRegex = "([a-zA-Z]{2,8})(?:[\\-_]([a-zA-Z]{4}))?(?:[\\-_]([a-zA-Z]{2}|[0-9]{3}))?(?:[\\-_]([^\\-_]+))?".r
  def buildLocale(str: String): Locale = {
    val LocaleRegex(lang, script, country, variant) = str: @unchecked
    val builder = new Locale.Builder
    builder.setLanguage(lang)
    builder.setScript(script)
    builder.setRegion(country)
    builder.setVariant(variant)
    builder.build()
  }

  private def prependList[E](l: java.util.List[E], e: E) = {
    java.util.List.copyOf(l.asScala.+:(e).asJava)
  }

  class ResourceBundleLoader(configPath: String, config: Config, wabase: WabaseApp[_] with DbAccess) extends
    ResourceBundle.Control {
    protected val fallbackLocale: Locale =
      if (config.getIsNull("fallback-locale")) null else buildLocale(config.getString("fallback-locale"))
    protected val timeToLive: Long = config.getDuration("time-to-live").toMillis
    protected val tunablePaths: Set[String] = Set("cp", "query", "fallback-locale", "time-to-live")
    protected val componentConfs: ComponentConfs = ComponentConf.getConfigs(configPath, tunablePaths)
    protected val configs: Seq[(String, ResourceBundle.Control)] = {
      val name_loader = componentConfs.confs.map { case (name, conf) =>
        @annotation.nowarn("msg=Manifest")
        val loader = getObjectOrNewInstance[ResourceBundle.Control](
          conf, "bundle-loader-class", "resource bundle loader",
          // runtime classes: `WabaseApp[_] with DbAccess` erases to WabaseApp in Scala 2 but to DbAccess in Scala 3
          Seq(s"$configPath.$name", conf, wabase))
        (name, loader)
      }
      bundleOrder().flatMap(n => name_loader.get(n).map(n -> _))
    }
    private val name = configPath.substring(configPath.lastIndexOf(".") + 1)
    private def bundleOrder(): Seq[String] = config.getValue("bundle-order").unwrapped() match {
      case s: String => s.split(",").map(_.trim).toSeq
      case l: java.util.List[_] => l.asScala.map(String.valueOf).toSeq
      case x => sys.error(s"Invalid bundle-order value, expected array of strings, or comma separated strings, got: '$x'")
    }

    private def firstBundle(bl: ResourceBundle.Control, name: String, locale: Locale,
                            loader: ClassLoader, reload: Boolean): ResourceBundle = {
      @tailrec
      def fb(formats: List[String]): ResourceBundle = formats match {
        case Nil => null
        case format :: tail =>
          val b = bl.newBundle(name, locale, format, loader, reload)
          if (b != null) b else fb(tail)
      }
      fb(bl.getFormats(name).asScala.toList)
    }

    override def getFormats(baseName: String): util.List[String] = util.Collections.singletonList("wabase-i18n")

    override def getFallbackLocale(baseName: String, locale: Locale): Locale =
      if (fallbackLocale == null) super.getFallbackLocale(baseName, locale)
      else if (locale.equals(fallbackLocale)) null else fallbackLocale

    override def getTimeToLive(baseName: String, locale: Locale): Long = timeToLive

    override def needsReload(baseName: String, locale: Locale, format: String,
                             loader: ClassLoader, bundle: ResourceBundle, loadTime: Long): Boolean = true

    override def newBundle(
      baseName: String,
      locale: Locale,
      format: String,
      loader: ClassLoader,
      reload: Boolean): ResourceBundle = {
      if (baseName == name)
        if (configs.nonEmpty) {
          val bundles = configs
            .map { case (name, bl) => firstBundle(bl, name, locale, loader, reload) }
            .filter(_ != null)
          if (bundles.nonEmpty) new CombinedResourceBundle(bundles) else null
        } else {
          val cp = if (config.getIsNull("cp")) null else config.getString("cp")
          val query = if (config.getIsNull("query")) null else config.getString("query")
          firstBundle(new DbBundleControl(wabase, cp, query, timeToLive, fallbackLocale),
            baseName, locale, loader, reload)
        }
      else configs
        .collectFirst { case (cn, bl) if cn == baseName =>
          firstBundle(bl, baseName, locale, loader, reload)
        }
        .orNull
    }
  }

  class CombinedResourceBundle(bundles: Seq[ResourceBundle]) extends ListResourceBundle {
    override def getContents: Array[Array[AnyRef]] = {
      bundles
        .foldLeft(ArrayBuffer[Array[AnyRef]]() -> Set[String]()) { case (r, b) =>
          b.getKeys.asScala.foldLeft(r) { case (r1@(res, exk), k) =>
            if (exk(k)) r1
            else (res += Array(k, b.getObject(k)), exk + k)
          }
        }
        ._1.toArray
    }
  }
  class IteratorResourceBundle(it: Iterator[(String, AnyRef)]) extends ListResourceBundle {
    override def getContents: Array[Array[AnyRef]] =
      it.map { case (key, value) => Array(key, value) }.toArray
  }
  class YamlBundleControl(ttl: Long, fallbackLoc: Locale) extends ResourceBundle.Control {
    override def getTimeToLive(baseName: String, locale: Locale): Long = ttl
    override def getFallbackLocale(baseName: String, locale: Locale): Locale = fallbackLoc
    override def getFormats(baseName: String): util.List[String] =
      prependList(super.getFormats(baseName), "yaml")

    override def newBundle(
      baseName: String,
      locale: Locale,
      format: String,
      loader: ClassLoader,
      reload: Boolean
    ): ResourceBundle = {
      if (format == "yaml") {
        def loadYaml(in: java.io.InputStream): Iterator[(String, AnyRef)] = {
          val settings = LoadSettings.builder()
            .setLabel("i18n bundle")
            .setAllowDuplicateKeys(false)
            .build()
          new Load(settings).loadFromInputStream(in) match {
            case null => null
            case m: java.util.Map[String, AnyRef]@unchecked => m.asScala.iterator
            case x => sys.error("Expected Map[String, Any], got class: " + x.getClass)
          }
        }
        val fileName = toResourceName(toBundleName(baseName, locale), "yaml")
        val in = loader.getResourceAsStream(fileName)
        val it = if (in != null) try loadYaml(in) finally in.close() else null
        if (it != null) new IteratorResourceBundle(it) else null
      } else {
        super.newBundle(baseName, locale, format, loader, reload)
      }
    }
  }
  class DbBundleControl(
    dbAccess: DbAccess,
    cp: String,
    query: String,
    ttl: Long,
    fallbackLoc: Locale
  ) extends YamlBundleControl(ttl, fallbackLoc) {
    override def getFormats(baseName: String): util.List[String] =
      prependList(super.getFormats(baseName), "jdbc")

    override def newBundle(
      baseName: String,
      locale: Locale,
      format: String,
      loader: ClassLoader,
      reload: Boolean
    ): ResourceBundle = {
      if (format == "jdbc") {
        if (cp != null && query != null) {
          @annotation.nowarn("msg=Manifest")
          val content = dbAccess.withConn(PoolName(cp)) { implicit res =>
            import org.tresql.CoreTypes._   // for scala 3
            Query(query)(res.withParams(Map("name" -> baseName, "locale" -> locale.toString)))
              .list[(String, String)]
          }
          if (content.nonEmpty) new IteratorResourceBundle(content.iterator)
          else null
        } else null
      } else super.newBundle(baseName, locale, format, loader, reload)
    }
  }
}

object I18nService {
  val ApplicationLanguageCookiePostfix = config.getString("app.language-cookie-postfix")

  def currentLangFromHeader(request: HttpRequest): Option[String] = {
    LanguageNegotiator(request.headers)
      .acceptedLanguageRanges
      .headOption
      .map(l => l.primaryTag +: l.subTags)
      .map(_.mkString("-"))
  }

  def applicationLocale(state: ApplicationState): Locale =
    state.state.get(AppServiceBase.ApplicationStateCookiePrefix + ApplicationLanguageCookiePostfix)
      .map(l => I18n.buildLocale(String.valueOf(l)))
      .getOrElse(Locale.getDefault)

  implicit def i18BundleMarshaller: ToEntityMarshaller[I18Bundle] = Marshaller.combined { bundle =>
    val source = ResultSerializer.source(
      () => bundle.bundle,
      os => BorerNestedArraysEncoder(os, Json, wrap = true, encoder => {
        case (k: String, v: String) =>
          encoder.w.writeMapStart()
          encoder.writeValue(k)
          encoder.writeValue(v)
          encoder.writeBreak()
          encoder.w
      })
    )
    HttpEntity.Chunked.fromData(`application/json`, source)
  }
}
