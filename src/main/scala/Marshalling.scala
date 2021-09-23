package org.wabase

import akka.http.scaladsl.util.FastFuture
import akka.http.scaladsl._
import model._
import MediaTypes._
import marshalling._
import spray.json.JsValue
import java.io.Writer
import java.net.URLEncoder
import java.text.Normalizer
import java.util.zip.ZipOutputStream

import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}
import spray.json._
import DefaultJsonProtocol._
import akka.http.scaladsl.model.headers.{ContentDispositionType, ContentDispositionTypes, `Content-Disposition`}
import akka.http.scaladsl.model.headers.{Location, RawHeader}
import akka.http.scaladsl.server.ContentNegotiator
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, FromResponseUnmarshaller, Unmarshaller}
import akka.stream.scaladsl.Source
import akka.util.ByteString
import org.wabase.Format.{xlsxDateTime, xsdDate}

import scala.collection.immutable.{Seq => iSeq}
import scala.language.implicitConversions
import scala.language.reflectiveCalls
import scala.util.Try
import scala.util.control.NonFatal

trait Marshalling extends DtoMarshalling
  with TresqlResultMarshalling
  with BasicJsonMarshalling
  with BasicMarshalling
  with QuereaseResultMarshalling { this: AppServiceBase[_] with Execution => }

trait BasicJsonMarshalling extends akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport with BasicMarshalling {
  this: JsonConverterProvider =>

  import jsonConverter._

  implicit val mapMarshaller: ToEntityMarshaller[Map[String, Any]] = Marshaller.combined(_.toJson)

  implicit val mapListMarshaller: ToEntityMarshaller[List[Map[String, Any]]] = Marshaller.combined(_.toJson)

  implicit def mapFutureMarshaller: ToEntityMarshaller[Future[Map[String, Any]]] =
    combinedWithEC(ec => mapF => mapF.map(_.toJson)(ec))

  implicit def mapUnmarshaller(implicit jsonUnmarshaller: FromEntityUnmarshaller[JsValue]): FromEntityUnmarshaller[Map[String, Any]] =
    jsonUnmarshaller.map(_.convertTo[Map[String, Any]])

  implicit def jsObjectUnmarshaller(implicit jsonUnmarshaller: FromEntityUnmarshaller[JsValue]) = jsonUnmarshaller.map(_.asJsObject)
}

trait BasicMarshalling {

  // why this is not the method in Marshaller, like it has composeWithEC and wrapWithEC ???
  def combinedWithEC[A, B, C](marshal: ExecutionContext => A => B)(implicit m2: Marshaller[B, C]): Marshaller[A, C] =
    Marshaller[A, C] { ec => a => m2.composeWithEC(marshal).apply(a)(ec) }

  implicit def optionUnmarshaller[T](implicit unm: FromResponseUnmarshaller[T]): FromResponseUnmarshaller[Option[T]] =
    Unmarshaller.withMaterializer{implicit ec => implicit mat => entity =>
      if (entity.status == StatusCodes.NotFound || entity.status == StatusCodes.NoContent) Future.successful(None) else unm(entity).map(r => Option(r))
    }

  implicit def optionMarshaller[A](implicit m: ToResponseMarshaller[A]): ToResponseMarshaller[Option[A]] = Marshaller { implicit ec => {
    case Some(value) => m(value)
    case None => FastFuture.successful(Marshalling.Opaque(() => HttpResponse(StatusCodes.NotFound)) :: Nil)
  }}

  implicit def TupleUnmarshaller[A, B, P](implicit ma: Unmarshaller[P, A], mb: Unmarshaller[P, B]): Unmarshaller[P, (A, B)] =
    Unmarshaller.withMaterializer { implicit ec => implicit mat => resp =>
      val resA = ma(resp)
      val resB = mb(resp)
      for{
        a <- resA
        b <- resB
      }yield(a, b)
    }
  implicit val ContentTypeUnmarshaller: FromEntityUnmarshaller[ContentType] = Unmarshaller{implicit ec => entity => Future.successful(entity.contentType)}
  implicit val StatusCodeUnmarshaller: FromResponseUnmarshaller[StatusCode] = Unmarshaller{implicit ec => response => Future.successful(response.status)}
  implicit val SourceUnmarshaller: FromEntityUnmarshaller[Source[ByteString, Any]] = Unmarshaller{implicit ec => entity => Future.successful(entity.dataBytes)}
  implicit val HeadersUnmarshaller: FromResponseUnmarshaller[iSeq[HttpHeader]] = Unmarshaller{implicit ec => resp => Future.successful(resp.headers)}
  implicit val UnitUnmarshaller: FromResponseUnmarshaller[Unit] = Unmarshaller.withMaterializer{implicit ec => implicit mat => resp => Future.successful(resp.entity.discardBytes())}

  case class GeneratedFile(name: String, contentType: ContentType, content: Array[Byte], contentDispositionType: ContentDispositionType = ContentDispositionTypes.attachment)
  case class StreamedFile(name: String, contentType: ContentType, content: Source[ByteString, Any], contentDispositionType: ContentDispositionType = ContentDispositionTypes.attachment)

  /*    `Content-Disposition`(ContentDispositionTypes.attachment, Map("filename" -> "...", "filename*" -> "...")
        // TODO content disposition akka-http bug: https://github.com/akka/akka-http/issues/1240
      ))*/
  def contentDisposition(fileName: String, dispositionType: ContentDispositionType) = {
    // US-ASCII visual chars except for '"' and escape chars '\' and (for faulty clients) '%'. Placeholder for other chars
    // https://www.greenbytes.de/tech/webdav/rfc7230.html#rule.quoted-string
    // https://tools.ietf.org/html/rfc6266#appendix-D
    val asciiFileName = fallbackFilename(fileName).toList
      .map(c => if (c >= ' ' && c <= '~' && c != '\\' && c != '%' && c != '\"') c else '?').toSeq.mkString
    val extended =
      if   (fileName == asciiFileName) ""
      // Can be left unencoded: alpha, digit, !#$&+-.^_`|~
      // https://www.greenbytes.de/tech/webdav/rfc8187.html#definition
      // URLEncoder encodes more, but converts space to '+' and leaves '*' unencoded
      else s"""; filename*=UTF-8''${URLEncoder.encode(fileName, "UTF-8").replace("+", "%20").replace("*", "%2A")}"""
    val dispositionValue = s"""$dispositionType; filename="$asciiFileName"""" + extended
    // Use RawHeader because akka-http puts value of extended `filename*` parameter in double quotes
    List(RawHeader("Content-Disposition", dispositionValue))
  }
  def fallbackFilename(filename: String) = stripAccents(filename)

  def stripAccents(s: String) = {
    val DiacriticsRegex = "\\p{InCombiningDiacriticalMarks}+".r
    DiacriticsRegex.replaceAllIn(Normalizer.normalize(s, Normalizer.Form.NFD), "")
  }

  implicit val generatedFileMarshaller: ToResponseMarshaller[GeneratedFile] = Marshaller.combined(file =>
    HttpResponse(StatusCodes.OK, contentDisposition(file.name, file.contentDispositionType), HttpEntity(file.contentType, file.content))
  )

  implicit val streamedFileMarshaller: ToResponseMarshaller[StreamedFile] = Marshaller.combined(file =>
    HttpResponse(StatusCodes.OK, contentDisposition(file.name, file.contentDispositionType), HttpEntity(file.contentType, file.content))
  )
}

trait AppMarshalling { this: AppServiceBase[_] with Execution =>

  trait AbstractChunker extends RowWriter {
    type Obj <: {def toMap: Map[String, Any]}
    type Result <: Iterator[Obj] with AutoCloseable

    def labels: Seq[String]
    def row(r: Obj): Unit
    def result: Result
    override def hasNext = result.hasNext
    override def row() = row(result.next())
    override def close() = result.close()
  }

  class JsonListChunker[T: JsonFormat](val result: Iterator[T], writer: Writer) extends RowWriter {
    override def header() = writer write "["

    var first = true
    override def row(): Unit = {
      if(first) first = false else writer write ",\n"
      writer write result.next().toJson.compactPrint
    }

    override def footer() = writer write "]\n"

    def hasNext = result.hasNext
    def close() = {}
  }

  val dbBufferSize = 1024 * 32
  val dbDataFileMaxSize = MarshallingConfig.dbDataFileMaxSize

  abstract class OdsChunker(zos: ZipOutputStream) extends AbstractChunker {

    import org.wabase.spreadsheet.ods._

    val streamer = new OdsStreamer(zos)

    override def header() = {
      streamer.startWorkbook
      streamer.startWorksheet
      streamer.startTable("dati")
      streamer.startRow
      labels foreach { h => streamer.cell(h) }
      streamer.endRow
    }
    override def row(r: Obj) = {
      streamer.startRow
      r.toMap.values foreach { v: Any => streamer.cell(v) }
      streamer.endRow
    }
    override def footer() = {
      streamer.endTable
      streamer.endWorksheet
      streamer.endWorkbook
    }
  }

  protected def source(src: Source[ByteString, _], maxFileSize: Long): Future[SourceValue] = {
    RowSource.value(dbBufferSize, maxFileSize, src)
  }

  def httpResponse(
    contentType: ContentType,
    src: Source[ByteString, _],
    maxFileSize: Long = dbDataFileMaxSize,
    transform: HttpResponse => HttpResponse = identity,
  )(implicit ec: ExecutionContext) = {
    source(src, maxFileSize).map {
      case CompleteSourceValue(data) => transform(HttpResponse(entity = HttpEntity.Strict(contentType, data)))
      case IncompleteSourceValue(sourceData) => transform(HttpResponse(entity = HttpEntity.Chunked.fromData(contentType, sourceData)))
    }.recover { case InsufficientStorageException(msg) =>
      HttpResponse(status = StatusCodes.InsufficientStorage,
        entity = HttpEntity.Strict(ContentTypes.`text/plain(UTF-8)`, ByteString(msg)))
    }
  }

  import RowSource._
  implicit def toResponseListJsonMarshaller[T: JsonFormat]: ToResponseMarshaller[Iterator[T]] =
    responseMarshaller(`application/json`, iterator => new JsonListChunker(iterator, _: Writer))

  def responseMarshaller[T](
    contentType: ContentType,
    toSource: T => Source[ByteString, _],
    toMaxFileSize: T => Long = (_: T) => dbDataFileMaxSize,
    transform: HttpResponse => HttpResponse = identity,
  ): ToResponseMarshaller[T] =
    Marshaller.combined((x: T) =>
      httpResponse(contentType, toSource(x), toMaxFileSize(x), transform)
    )(GenericMarshallers.futureMarshaller(Marshaller.withFixedContentType(contentType)(identity)))
}

trait TresqlResultMarshalling extends AppMarshalling { this: AppServiceBase[_] with Execution =>
  import org.tresql.{Result => TresqlResult, RowLike}

  trait AbstractTresqlResultChunker extends AbstractChunker {
    override type Obj = RowLike
    override type Result = TresqlResult[Obj]
    def labels = result.columns.map(_.name).toVector
  }

  class OdsTresqlResultChunker(val result: TresqlResult[RowLike], zos: ZipOutputStream) extends OdsChunker(zos) with AbstractTresqlResultChunker
  class CsvTresqlResultChunker(val result: TresqlResult[RowLike], writer: Writer) extends AbstractTresqlResultChunker {
    def escapeValue(s: String) =
      if (s == null) null
      else if (s.contains(",") || s.contains("\"")) ("\"" + s.replaceAll("\"", "\"\"") + "\"")
      else s

    override def header() = {
      writer.write(labels.map(escapeValue).mkString("",",","\n"))
      writer.flush
    }
    override def row(r: Obj) = {
      writer.write(r.rowToVector.map(v => csvValue(v)).mkString("",",","\n"))
      writer.flush
    }
    override def footer() = {}

    def csvValue(v: Any): String = Option(v).map{
      case m: Map[String @unchecked, Any @unchecked] => ""
      case l: Traversable[Any] => ""
      //case d: DTO @unchecked => ""
      case n: java.lang.Number => String.valueOf(n)
      case t: Timestamp => xlsxDateTime(t)
      case d: jDate => xsdDate(d)
      case x => x.toString
    }.map(escapeValue).getOrElse("")
  }

  import RowSource._
  val toResponseTresqlResultOdsMarshaller: ToResponseMarshaller[TresqlResult[RowLike]] =
    responseMarshaller(`application/vnd.oasis.opendocument.spreadsheet`, result => new OdsTresqlResultChunker(result, _))

  val toResponseTresqlResultCsvMarshaller: ToResponseMarshaller[TresqlResult[RowLike]] =
    responseMarshaller(ContentTypes.`text/plain(UTF-8)`, result => new CsvTresqlResultChunker(result, _))

  implicit val toResponseTresqlResultMarshaller: ToResponseMarshaller[TresqlResult[RowLike]] =
    Marshaller.oneOf(
      toResponseTresqlResultOdsMarshaller,
      toResponseTresqlResultCsvMarshaller,
    )
}

trait DtoMarshalling extends AppMarshalling with Loggable { this: AppServiceBase[_] with Execution =>

  import jsonConverter._
  import AppMetadata._

  import app.qe
  implicit class Wrapper(dto: app.Dto) {
    def toMap = dto.toMap
  }

  implicit def dtoUnmarshaller[T <: app.Dto](implicit jsonUnmarshaller: FromEntityUnmarshaller[JsValue], m: Manifest[T]): FromEntityUnmarshaller[T] =
    jsonUnmarshaller.map(js => m.runtimeClass.getConstructor().newInstance().asInstanceOf[T].fill(js.asJsObject()))

  implicit def dtoListUnmarshaller[T <: app.Dto](implicit jsonUnmarshaller: FromEntityUnmarshaller[JsValue], m: Manifest[T]): FromEntityUnmarshaller[List[T]] =
    jsonUnmarshaller.map(responseJson =>
      responseJson.convertTo[List[Any]].map { csJs =>
        m.runtimeClass.getConstructor().newInstance().asInstanceOf[T].fill(csJs.asInstanceOf[Map[String, Any]].toJson.asJsObject())
      }
    )

  trait AbstractDtoChunker extends AbstractChunker {
    override type Obj = Wrapper
    override type Result = Iterator[Wrapper] with AutoCloseable {def view: app.qe.ViewDef}
    def labels = result.view.fields.map(f => Option(f.label).getOrElse(f.name))
  }

  class JsonDtoChunker(val result: AbstractDtoChunker#Result, writer: Writer) extends AbstractDtoChunker {
    override def header() = writer write "["

    var first = true
    override def row(r: Wrapper): Unit = {
      if(first) first = false else writer write ",\n"
      writer write r.toMap.toJson.compactPrint
    }

    override def footer() = writer write "]\n"
  }

  class XlsXmlDtoChunker(val result: AbstractDtoChunker#Result, writer: Writer) extends AbstractDtoChunker {

    import org.wabase.spreadsheet.xlsxml._

    val headerStyle = Style("header", null, Font.BOLD)
    val streamer = new XlsXmlStreamer(writer)

    override def header() = {
      streamer.startWorkbook(Seq(headerStyle))
      streamer.startWorksheet("dati")
      streamer.startTable
      streamer.startRow
      labels foreach { h => streamer.cell(h, headerStyle) }
      streamer.endRow
    }
    override def row(r: Wrapper) = {
      streamer.startRow
      r.toMap.values foreach { v: Any => streamer.cell(v) }
      streamer.endRow
    }
    override def footer() = {
      streamer.endTable
      streamer.endWorksheet
      streamer.endWorkbook
    }
  }

  class OdsDtoChunker(val result: AbstractDtoChunker#Result, zos: ZipOutputStream) extends OdsChunker(zos) with AbstractDtoChunker

  implicit def dtoResultToWrapper(res: app.AppListResult[app.Dto]): AbstractDtoChunker#Result = new Iterator[Wrapper] with AutoCloseable {
    override def hasNext = res.hasNext
    override def next() = res.next()
    def view = res.view
    override def close = res.close
  }

  class CsvDtoChunker(val result: AbstractDtoChunker#Result, writer: Writer) extends AbstractDtoChunker {
    def escapeValue(s: String) =
      if (s == null) null
      else if (s.contains(",") || s.contains("\"")) ("\"" + s.replaceAll("\"", "\"\"") + "\"")
      else s

    override def header() = {
      writer.write(labels.map(escapeValue).mkString("",",","\n"))
      writer.flush
    }
    override def row(r: Wrapper) = rowWriter(r.toMap)
    override def footer() = {}

    def rowWriter(m: Map[String, Any]) = {
      writer.write(m.values.map(v => csvValue(v)).mkString("",",","\n"))
      writer.flush
    }

    def csvValue(v: Any): String = Option(v).map{
      case m: Map[String @unchecked, Any @unchecked] => ""
      case l: Traversable[Any] => ""
      case d: app.Dto @unchecked => ""
      case n: java.lang.Number => String.valueOf(n)
      case t: Timestamp => xlsxDateTime(t)
      case d: jDate => xsdDate(d)
      case x => x.toString
    }.map(escapeValue).getOrElse("")
  }

  import RowSource._

  protected def resultMaxFileSize(result: app.AppListResult[_]): Long =
    Try(result.view.name)
      .map(MarshallingConfig.customDataFileMaxSizes.getOrElse(_, dbDataFileMaxSize))
      .recover {
        //this can happen if view is of Nothing type as in a result of Nil conversion to AppListResult
        case NonFatal(e) =>
          logger.warn(s"Error getting data file buffer max size for view, " +
            s"using default value $dbDataFileMaxSize", e)
          dbDataFileMaxSize
      }.get


  val toResponseAppListResultJsonMarshaller: ToResponseMarshaller[app.AppListResult[app.Dto]]  = responseMarshaller(
    `application/json`, result => new JsonDtoChunker(result, _), resultMaxFileSize)
  val toResponseAppListResultExcelMarshaller: ToResponseMarshaller[app.AppListResult[app.Dto]] = responseMarshaller(
    `application/vnd.ms-excel`, result => new XlsXmlDtoChunker(result, _), resultMaxFileSize)
  val toResponseAppListResultOdsMarshaller: ToResponseMarshaller[app.AppListResult[app.Dto]]   = responseMarshaller(
    `application/vnd.oasis.opendocument.spreadsheet`, result => new OdsDtoChunker(result, _), resultMaxFileSize)
  val toResponseAppListResultCsvMarshaller: ToResponseMarshaller[app.AppListResult[app.Dto]]   = responseMarshaller(
    ContentTypes.`text/plain(UTF-8)`, result => new CsvDtoChunker(result, _), resultMaxFileSize)

  implicit val toResponseAppListResultMarshaller: ToResponseMarshaller[app.AppListResult[app.Dto]] =
    Marshaller.oneOf(
      toResponseAppListResultJsonMarshaller,
      toResponseAppListResultOdsMarshaller,
      toResponseAppListResultExcelMarshaller,
      toResponseAppListResultCsvMarshaller,
    )

  implicit val dtoMarshaller: ToEntityMarshaller[app.Dto] = Marshaller.withFixedContentType(`application/json`) {
    dto => HttpEntity.Strict(`application/json`, ByteString(new Wrapper(dto).toMap.toJson.compactPrint))
  }
}

trait QuereaseResultMarshalling { this:
    TresqlResultMarshalling with
    BasicJsonMarshalling    with
    DtoMarshalling          with
    AppServiceBase[_] =>

  import app.qe
  import app.qe.ListJsonFormat
  import app.qe.QuereaseIdResultJsonFormat
  implicit val toResponseQuereaseTresqlResultMarshaller:    ToResponseMarshaller[TresqlResult]   =
    Marshaller.combined(_.result)
  implicit val toEntityQuereaseMapResultMarshaller:         ToEntityMarshaller  [MapResult]      =
    Marshaller.combined(_.result)
  implicit val toEntityQuereasePojoResultMarshaller:        ToEntityMarshaller  [PojoResult]     =
    Marshaller.combined(_.result.toMap)
  implicit val toEntityQuereaseListResultMarshaller:        ToEntityMarshaller  [ListResult]     =
    Marshaller.combined(_.result.toJson)
  implicit val toEntityQuereaseIteratorResultMarshaller:    ToResponseMarshaller[IteratorResult] =
    Marshaller.combined(_.result.asInstanceOf[app.AppListResult[app.Dto]]) // FIXME asInstanceOf
  implicit val toResponseQuereaseOptionResultMarshaller:    ToResponseMarshaller[OptionResult]   =
    Marshaller.combined(_.result.map(_.toMap))
  implicit val toEntityQuereaseNumberResultMarshaller:      ToEntityMarshaller  [NumberResult]   =
    Marshaller.combined(id => s"$id")
  implicit val toEntityQuereaseCodeResultMarshaller:        ToEntityMarshaller  [CodeResult]     =
    Marshaller.combined(_.code)
  implicit val toEntityQuereaseIdResultMarshaller:          ToEntityMarshaller  [IdResult]       =
    Marshaller.combined(_.toJson)
  implicit val toResponseQuereaseRedirectResultMarshaller:  ToResponseMarshaller[RedirectResult] =
    Marshaller.combined(rr => (StatusCodes.SeeOther, Seq(Location(rr.uri))))
  implicit val toEntityQuereaseNoResultMarshaller:          ToEntityMarshaller  [NoResult.type]  =
    Marshaller.combined(_ => "")

  implicit val toResponseQuereaseResultMarshaller: ToResponseMarshaller[QuereaseResult] =
    Marshaller { implicit ec => value => value match {
      case tq: TresqlResult   => (toResponseQuereaseTresqlResultMarshaller:   ToResponseMarshaller[TresqlResult]  )(tq)
      case mp: MapResult      => (toEntityQuereaseMapResultMarshaller:        ToResponseMarshaller[MapResult]     )(mp)
      case pj: PojoResult     => (toEntityQuereasePojoResultMarshaller:       ToResponseMarshaller[PojoResult]    )(pj)
      case ls: ListResult     => (toEntityQuereaseListResultMarshaller:       ToResponseMarshaller[ListResult]    )(ls)
      case it: IteratorResult => (toEntityQuereaseIteratorResultMarshaller:   ToResponseMarshaller[IteratorResult])(it)
      case op: OptionResult   => (toResponseQuereaseOptionResultMarshaller:   ToResponseMarshaller[OptionResult]  )(op)
      case nr: NumberResult   => (toEntityQuereaseNumberResultMarshaller:     ToResponseMarshaller[NumberResult]  )(nr)
      case cd: CodeResult     => (toEntityQuereaseCodeResultMarshaller:       ToResponseMarshaller[CodeResult]    )(cd)
      case id: IdResult       => (toEntityQuereaseIdResultMarshaller:         ToResponseMarshaller[IdResult]      )(id)
      case rd: RedirectResult => (toResponseQuereaseRedirectResultMarshaller: ToResponseMarshaller[RedirectResult])(rd)
      case no: NoResult.type  => (toEntityQuereaseNoResultMarshaller:         ToResponseMarshaller[NoResult.type] )(no)
      case xx                 => sys.error(s"QuereaseResult marshaller for class ${xx.getClass.getName} not implemented")
    }}
}

object MarshallingConfig extends AppBase.AppConfig with Loggable {
  lazy val dbDataFileMaxSize: Long =
    if (appConfig.hasPath("db-data-file-max-size"))
      appConfig.getBytes("db-data-file-max-size")
    else 1024 * 1024 * 8L

  import scala.jdk.CollectionConverters._
  lazy val customDataFileMaxSizes: Map[String, Long] = {
    val vals = Try {
      appConfig.getConfig("db-data-file-max-sizes")
        .entrySet.asScala.map(e => e.getKey -> e.getValue.atKey("x").getBytes("x").toLong)
        .toMap
    }.toOption.getOrElse(Map())
    logger.debug(s"Custom db data max file sizes: $vals")
    vals
  }
}
