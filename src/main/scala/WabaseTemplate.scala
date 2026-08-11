package org.wabase

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{FileIO, StreamConverters}
import org.apache.pekko.util.ByteString
import com.samskivert.mustache.{Mustache, Template}
import org.tresql.SimpleCacheBase
import org.xhtmlrenderer.pdf.{ITextOutputDevice, ITextRenderer, ITextUserAgent}
import org.xhtmlrenderer.util.{FontUtil, ImageUtil}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream}
import java.net.{URI, URLDecoder}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.Try

trait WabaseTemplate {
  def apply(template: String, byName: Boolean, data: Iterable[_], targetName: String)(implicit
    ec: ExecutionContext,
    as: ActorSystem,
    fs: FileStreamer,
  ): Future[TemplateResult]
}

class DefaultWabaseTemplate extends WabaseTemplate {
  @annotation.nowarn("msg=Manifest")
  protected val loader: WabaseTemplateLoader =
    factory[WabaseTemplateLoader]("app.template.loader")
  @annotation.nowarn("msg=Manifest")
  protected val renderer: WabaseTemplateRenderer =
    factory[WabaseTemplateRenderer]("app.template.renderer")
  private def factory[T](propName: String)(implicit m: Manifest[T]): T = {
    getObjectOrNewInstance[T](config, propName, "template factory")
  }
  override def apply(template: String, byName: Boolean, data: Iterable[_], targetName: String)(implicit
    ec: ExecutionContext,
    as: ActorSystem,
    fs: FileStreamer
  ): Future[TemplateResult] = {
    (if (byName) loader.load(template) else Future.successful(template.getBytes("UTF-8")))
      .flatMap { renderer(targetName, _, data) }
  }
}

trait WabaseTemplateLoader {
  def load(template: String)(implicit
    ec: ExecutionContext,
    as: ActorSystem,
    fs: FileStreamer,
  ): Future[Array[Byte]]
}

class DefaultWabaseTemplateLoader extends WabaseTemplateLoader {
  val TemplateDirParam       = "app.template.dir"
  val ClasspathPrefixParam   = "app.template.classpath-prefix"
  val fn_reg_ex = """(\d+?)/([0-9a-fA-F]{64})$""".r // filename in form: id/sha256

  val template_dir: String =
    if (config.hasPath(TemplateDirParam)) config.getString(TemplateDirParam) else null

  /** Absolute normalized filesystem root for templates; None if `app.template.dir` is unset. */
  lazy val templateDirPath: Option[Path] =
    Option(template_dir).map(_.trim).filter(_.nonEmpty).map(p => Paths.get(p).toAbsolutePath.normalize)

  /**
   * Allowed classpath path prefix (normalized to start and end with '/').
   * Empty / unset disables classpath template loading.
   */
  lazy val classpathPrefix: Option[String] = {
    if (!config.hasPath(ClasspathPrefixParam) || config.getIsNull(ClasspathPrefixParam)) None
    else {
      val raw = config.getString(ClasspathPrefixParam).trim
      if (raw.isEmpty) None
      else {
        val normalized = TemplatePathUtils.normalizeClasspathPath(raw)
        val withSlash = if (normalized.endsWith("/")) normalized else s"$normalized/"
        Option(withSlash).filter(_ != "/")
      }
    }
  }

  override def load(template: String)(implicit
    ec: ExecutionContext,
    as: ActorSystem,
    fs: FileStreamer,
  ): Future[Array[Byte]] = {
    loadFromFileStreamer(template)
      .orElse(loadFromFile(template))
      .orElse(loadFromResource(template))
      .getOrElse(throw new BusinessException(s"Template not found: $template"))
  }

  protected def loadFromFileStreamer(template: String)(implicit
    ec: ExecutionContext,
    as: ActorSystem,
    fs: FileStreamer,
  ): Option[Future[Array[Byte]]] = {
    Option(fs).filter(_ => fn_reg_ex.pattern.matcher(template).matches()).flatMap { fs =>
      val fn_reg_ex(id, sha) = template: @unchecked
      fs.getFileInfo(id.toLong, sha).map {
        _.source.runFold(ByteString.empty)(_ ++ _).map(_.toArray)
      }
    }
  }

  protected def loadFromFile(template: String)(implicit
    ec: ExecutionContext,
    as: ActorSystem
  ): Option[Future[Array[Byte]]] = {
    for {
      root <- templateDirPath
      path <- resolveUnderRoot(root, template) if Files.isRegularFile(path)
    } yield FileIO.fromPath(path).runFold(ByteString.empty)(_ ++ _).map(_.toArray)
  }

  protected def loadFromResource(template: String)(implicit
    ec: ExecutionContext,
    as: ActorSystem
  ): Option[Future[Array[Byte]]] = {
    classpathPathOf(template).flatMap { path =>
      Option(getClass.getResourceAsStream(path))
        .orElse(Option(getClass.getResourceAsStream(path.drop(1))))
    }.map { in =>
      StreamConverters.fromInputStream(() => in).runFold(ByteString.empty)(_ ++ _).map(_.toArray)
    }
  }

  /** Decode + reject traversal; require path under `root`. */
  protected def resolveUnderRoot(root: Path, template: String): Option[Path] =
    TemplatePathUtils.safeDecodedPath(template).map { decoded =>
      val raw = Paths.get(decoded)
      if (raw.isAbsolute) raw.toAbsolutePath.normalize
      else root.resolve(decoded).toAbsolutePath.normalize
    }.filter(_.startsWith(root))

  /** Decode + reject traversal; require configured classpath prefix. */
  protected def classpathPathOf(template: String): Option[String] =
    for {
      prefix <- classpathPrefix
      decoded <- TemplatePathUtils.safeDecodedPath(template)
      path = TemplatePathUtils.normalizeClasspathPath(decoded)
      if path != "/" && path.nonEmpty && path.startsWith(prefix)
    } yield path
}

/** Shared path checks for template bodies and PDF assets (path traversal / double-encoding). */
private[wabase] object TemplatePathUtils {
  def safeDecodedPath(raw: String): Option[String] = {
    Try(URLDecoder.decode(raw, StandardCharsets.UTF_8)).toOption.filterNot { decoded =>
      decoded.contains("..") || decoded.contains('%') || decoded.contains('\u0000')
    }
  }

  def normalizeClasspathPath(raw: String): String = {
    val trimmed = raw.trim
    val withSlash = if (trimmed.startsWith("/")) trimmed else s"/$trimmed"
    val normalized = Paths.get(withSlash).normalize.toString.replace('\\', '/')
    if (normalized.startsWith("/")) normalized else s"/$normalized"
  }
}

trait WabaseTemplateRenderer {
  def apply(templateName: String, template: Array[Byte], data: Iterable[_])(implicit ec: ExecutionContext): Future[TemplateResult]
}

class MustacheTemplateCache(maxSize: Int)
  extends SimpleCacheBase[Template](maxSize, "Mustache template cache")

/**
 * See http://mustache.github.io/mustache.5.html
 * */
class MustacheTemplateRenderer extends WabaseTemplateRenderer {
  protected val cache: Option[MustacheTemplateCache] =
    Some(new MustacheTemplateCache(256))
  override def apply(templateName: String, template: Array[Byte], data: Iterable[_])(implicit ec: ExecutionContext): Future[TemplateResult] = {
    Future.successful(StringTemplateResult(
      render(templateName, template, data)
    ))
  }
  def render(templateName: String, template: Array[Byte], data: Iterable[_]): String = {
    val templateString = new String(template, "UTF-8")
    render(templateName, templateString, data)
  }
  def render(templateName: String, templateString: String, data: Iterable[_]): String = {
    val context = MapUtils.mapToJavaMap(data match {
      case m: Map[String@unchecked, _]      => m
      case s: Seq[Map[String, _]@unchecked] => s.headOption.getOrElse(Map.empty) + ("items" -> data)
      case x =>
        val className = Option(x).map(_.getClass.getName).orNull
        sys.error(s"Unexpected template data class: $className. Expecting Map[String, _] or Seq[_]")
    })
    try {
      cache.flatMap(_.get(templateString)).getOrElse {
        val template =
          Mustache.compiler()
            .nullValue("")
            .compile(templateString)
        cache.foreach(_.put(templateString, template))
        template
      }
        .execute(context)
    } catch {
      case util.control.NonFatal(ex) =>
        throw new RuntimeException(s"Failed to render template '$templateString', " +
          s"data class: ${Option(data).map(_.getClass.getName).orNull}", ex)
    }
  }
}

class MustacheAndPdfTemplateRenderer extends MustacheTemplateRenderer {
  override def apply(templateName: String, template: Array[Byte], data: Iterable[_])(implicit ec: ExecutionContext): Future[TemplateResult] = {
    Future.successful {
      val s = super.render(templateName, template, data)
      if (templateName != null && templateName.endsWith(".pdf")) {
        val baos = new ByteArrayOutputStream
        PdfRenderer.render(s, baos)
        FileTemplateResult(templateName, "application/pdf", baos.toByteArray)
      } else {
        StringTemplateResult(s)
      }
    }
  }
}

object PdfRenderer {
  val AssetsDirectoriesParam       = "app.template.assets.directories"
  val AssetsClasspathPrefixesParam = "app.template.assets.classpath-prefixes"

  /** Allowed filesystem roots for template assets (images, fonts, CSS). */
  lazy val assetsDirectories: Seq[Path] =
    config.getStringList(AssetsDirectoriesParam).asScala.toSeq
      .map(_.trim).filter(_.nonEmpty)
      .map(p => Paths.get(p).toAbsolutePath.normalize)

  /** Allowed classpath path prefixes (normalized to start and end with '/'). */
  lazy val assetsClasspathPrefixes: Seq[String] =
    config.getStringList(AssetsClasspathPrefixesParam).asScala.toSeq
      .map(TemplatePathUtils.normalizeClasspathPath)
      .map(p => if (p.endsWith("/")) p else s"$p/")
      .filter(p => p != "/")

  /**
   * Loads PDF/HTML template assets without network or arbitrary filesystem access (SSRF-safe).
   * Allowed sources: `data:` URIs, classpath paths under configured prefixes,
   * and files under configured directory roots.
   */
  class TemplateAssetsLoader(outputDevice: ITextOutputDevice)
    extends ITextUserAgent(outputDevice, ITextRenderer.DEFAULT_DOTS_PER_PIXEL) {
    override def resolveAndOpenStream(uri: String): InputStream =
      openAsset(uri).orNull
  }

  def openAsset(uri: String): Option[InputStream] = {
    if (uri == null || uri.isEmpty) None
    else if (uri.regionMatches(true, 0, "data:", 0, 5)) openDataUri(uri)
    else openClasspathAsset(uri).orElse(openFileAsset(uri))
  }

  /** Only `data:font/` and `data:image/` base64 (flying-saucer FontUtil / ImageUtil). */
  protected def openDataUri(uri: String): Option[InputStream] =
    if (FontUtil.isEmbeddedBase64Font(uri))
      Option(FontUtil.getEmbeddedBase64Data(uri))
    else if (ImageUtil.isEmbeddedBase64Image(uri))
      Option(ImageUtil.getEmbeddedBase64Image(uri)).map(new ByteArrayInputStream(_))
    else None

  protected def openClasspathAsset(uri: String): Option[InputStream] = {
    classpathPathOf(uri).filter(isAllowedClasspathPath).flatMap { path =>
      Option(getClass.getResourceAsStream(path))
        .orElse(Option(getClass.getResourceAsStream(path.drop(1)))) // without leading '/'
    }
  }

  protected def openFileAsset(uri: String): Option[InputStream] = {
    filePathCandidates(uri)
      .find(p => isAllowedFilePath(p) && Files.isRegularFile(p))
      .map(Files.newInputStream(_))
  }

  protected def classpathPathOf(uri: String): Option[String] = {
    val pathOpt =
      if (uri.startsWith("classpath:"))
        Some(uri.substring("classpath:".length))
      else if (uri.startsWith("jar:")) {
        val bang = uri.indexOf("!/")
        if (bang >= 0) Some(uri.substring(bang + 1)) else None
      } else if (hasDisallowedScheme(uri))
        None
      else if (uri.startsWith("file:"))
        None
      else
        Some(uri)
    pathOpt
      .flatMap(safeDecodedPath)
      .map(normalizeClasspathPath)
      .filterNot(p => p == "/" || p.isEmpty)
  }

  protected def filePathCandidates(uri: String): Seq[Path] = {
    if (uri.startsWith("file:")) {
      Try {
        val u = new URI(uri)
        val rawPath =
          Option(u.getRawPath).filter(_.nonEmpty)
            .orElse(Option(u.getRawSchemeSpecificPart).filter(_.nonEmpty))
            .getOrElse("")
        safeDecodedPath(rawPath).map(_ => Paths.get(u).toAbsolutePath.normalize)
      }.toOption.flatten.toSeq
    } else if (schemeOf(uri).isDefined)
      Seq.empty
    else {
      safeDecodedPath(uri).map { decoded =>
        val raw = Paths.get(decoded)
        if (raw.isAbsolute) Seq(raw.toAbsolutePath.normalize)
        else assetsDirectories.map(_.resolve(decoded).toAbsolutePath.normalize)
      }.getOrElse(Seq.empty)
    }
  }

  protected def safeDecodedPath(raw: String): Option[String] =
    TemplatePathUtils.safeDecodedPath(raw)

  protected def isAllowedClasspathPath(path: String): Boolean =
    assetsClasspathPrefixes.exists(path.startsWith)

  protected def isAllowedFilePath(path: Path): Boolean =
    assetsDirectories.exists(dir => path.startsWith(dir))

  protected def hasDisallowedScheme(uri: String): Boolean =
    schemeOf(uri).exists { s =>
      val scheme = s.toLowerCase
      scheme != "classpath" && scheme != "file" && scheme != "jar" && scheme != "data"
    }

  protected def schemeOf(uri: String): Option[String] =
    Try(new URI(uri)).toOption.flatMap(u => Option(u.getScheme))

  protected def normalizeClasspathPath(raw: String): String =
    TemplatePathUtils.normalizeClasspathPath(raw)

  def render(htmlContent: String, outputStream: OutputStream) = {
    val renderer = new ITextRenderer

    val sharedContext = renderer.getSharedContext
    sharedContext.setPrint(true)
    sharedContext.setInteractive(false)
    // Register custom ReplacedElementFactory implementation
    sharedContext.getTextRenderer.setSmoothingThreshold(0)

    val assetsLoader = new TemplateAssetsLoader(renderer.getOutputDevice)
    // Prefer classpath resolution over CWD file: fallback used by NaiveUserAgent
    assetsLoader.setBaseURL("classpath:/")
    renderer.getSharedContext.setUserAgentCallback(assetsLoader)

    renderer.setDocumentFromString(htmlContent)
    renderer.layout
    renderer.createPDF(outputStream)
  }
}
