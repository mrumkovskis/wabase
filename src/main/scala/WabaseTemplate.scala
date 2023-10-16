package org.wabase

import akka.actor.ActorSystem
import akka.stream.scaladsl.{FileIO, StreamConverters}
import akka.util.ByteString

import java.nio.file.FileSystems
import scala.concurrent.{ExecutionContext, Future}

trait WabaseTemplate {
  def apply(template: String, data: Seq[Map[String, Any]])(implicit
    ec: ExecutionContext,
    as: ActorSystem,
    fs: FileStreamer,
  ): Future[TemplateResult]
}

class DefaultWabaseTemplate extends WabaseTemplate {
  protected val loader: WabaseTemplateLoader =
    factory[WabaseTemplateLoader]("app.template.loader")
  protected val renderer: WabaseTemplateRenderer =
    factory[WabaseTemplateRenderer]("app.template.renderer")
  private def factory[T](propName: String): T = {
    Class.forName(config.getString(propName)).newInstance().asInstanceOf[T]
  }

  override def apply(template: String, data: Seq[Map[String, Any]])(implicit
    ec: ExecutionContext,
    as: ActorSystem,
    fs: FileStreamer
  ): Future[TemplateResult] = {
    loader.load(template).flatMap { renderer(template, _, data) }
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
  val TemplateDirParam = "app.template.dir"
  val fn_reg_ex = """(\d+?)/([0-9a-fA-F]{64})$""".r // filename in form: id/sha256
  val template_dir = if (config.hasPath(TemplateDirParam)) config.getString(TemplateDirParam) else null

  override def load(template: String)(implicit
    ec: ExecutionContext,
    as: ActorSystem,
    fs: FileStreamer,
  ): Future[Array[Byte]] = {
    loadFromFileStreamer(template)
      .orElse(loadFromFile(template))
      .orElse(loadFromResource(template))
      .getOrElse(Future.successful(template.getBytes("UTF-8")))
  }

  protected def loadFromFileStreamer(template: String)(implicit
    ec: ExecutionContext,
    as: ActorSystem,
    fs: FileStreamer,
  ): Option[Future[Array[Byte]]] = {
    Option(fs).filter(_ => fn_reg_ex.pattern.matcher(template).matches()).flatMap { fs =>
      val fn_reg_ex(id, sha) = template
      fs.getFileInfo(id.toLong, sha).map {
        _.source.runFold(ByteString.empty)(_ ++ _).map(_.toArray)
      }
    }
  }

  protected def loadFromFile(template: String)(implicit
    ec: ExecutionContext,
    as: ActorSystem
  ): Option[Future[Array[Byte]]] = {
    Option(template_dir)
      .map(FileSystems.getDefault.getPath(_, template))
      .filter(_.toFile.exists)
      .map(FileIO.fromPath(_))
      .map {
        _.runFold(ByteString.empty)(_ ++ _).map(_.toArray)
      }
  }

  protected def loadFromResource(template: String)(implicit
    ec: ExecutionContext,
    as: ActorSystem
  ): Option[Future[Array[Byte]]] = {
    Option(getClass.getResourceAsStream(template))
      .map(in => StreamConverters.fromInputStream(() => in))
      .map {
        _.runFold(ByteString.empty)(_ ++ _).map(_.toArray)
      }
  }
}

trait WabaseTemplateRenderer {
  def apply(templateName: String, template: Array[Byte], data: Seq[Map[String, Any]]): Future[TemplateResult]
}
/**
 * Replaces all {{<key>}} in template with corresponding value from data.head
 * */
class SimpleTemplateRenderer extends WabaseTemplateRenderer {
  val templateRegex = """\{\{(\w+)\}\}""".r
  def apply(templateName: String, template: Array[Byte], data: Seq[Map[String, Any]]): Future[TemplateResult] = {
    val templ_str = new String(template, "UTF-8")
    val dataMap = data.head
    val (idx, res) = templateRegex.findAllMatchIn(templ_str).foldLeft(0 -> "") {
      case ((idx, r), m) =>
        (m.end, r + m.source.subSequence(idx, m.start) + String.valueOf(dataMap(m.group(1))))
    }
    Future.successful(StringTemplateResult(res + templ_str.substring(idx)))
  }
}
