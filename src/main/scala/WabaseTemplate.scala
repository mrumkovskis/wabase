package org.wabase

import akka.actor.ActorSystem
import akka.stream.scaladsl.{FileIO, StreamConverters}
import akka.util.ByteString
import com.samskivert.mustache.Mustache

import java.nio.file.FileSystems
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

trait WabaseTemplate {
  def apply(template: String, data: Seq[Map[String, Any]])(implicit
    ec: ExecutionContext,
    as: ActorSystem,
    fs: FileStreamer,
  ): Future[TemplateResult]
}

object WabaseTemplate {
  def mapToJavaMap(map: Map[String, _]):java.util.Map[String, _] = {
    val result = map.map { (entry: (String, _)) =>
      (entry._1,
        entry._2 match {
          case l: Seq[_] => seqToJavaList(l)
          case m: Map[String@unchecked, _] => mapToJavaMap(m)
          case r => r
        }
      )
    }
    result.asInstanceOf[Map[String, _]].asJava
  }

  def  seqToJavaList(seq: Seq[_]): java.util.List[_] = {
    val result = seq.toList.map {
      case l: Seq[_] => seqToJavaList(l)
      case m: Map[String @unchecked, _] => mapToJavaMap(m)
      case r => r
    }
    result.asJava
  }
}

class DefaultWabaseTemplate extends WabaseTemplate {
  protected val loader: WabaseTemplateLoader =
    factory[WabaseTemplateLoader]("app.template.loader")
  protected val renderer: WabaseTemplateRenderer =
    factory[WabaseTemplateRenderer]("app.template.renderer")
  private def factory[T](propName: String): T = {
    Class.forName(config.getString(propName)).getDeclaredConstructor().newInstance().asInstanceOf[T]
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
 * See http://mustache.github.io/mustache.5.html
 * */
class SimpleTemplateRenderer extends WabaseTemplateRenderer {
  import WabaseTemplate._
  def apply(templateName: String, template: Array[Byte], data: Seq[Map[String, Any]]): Future[TemplateResult] = {
    val templateString = new String(template, "UTF-8")
    val context = mapToJavaMap(
      data.headOption.getOrElse(Map.empty) + ("items" -> data)
    )
    Future.successful(StringTemplateResult(
      try {
        Mustache.compiler()
          .nullValue("")
          .compile(templateString) // TODO CACHE???
          .execute(context)
      } catch {
        case util.control.NonFatal(ex) =>
          throw new RuntimeException(s"Failed to render template '$templateString' with values $data", ex)
      }
    ))
  }
}
