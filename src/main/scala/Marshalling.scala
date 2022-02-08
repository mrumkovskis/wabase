package org.wabase

import akka.http.scaladsl.util.FastFuture
import akka.http.scaladsl._
import model._
import MediaTypes._
import marshalling._
import spray.json.JsValue

import java.io.{OutputStream, OutputStreamWriter, Writer}
import java.net.URLEncoder
import java.text.Normalizer
import java.util.zip.ZipOutputStream
import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}
import spray.json._
import DefaultJsonProtocol._
import akka.http.scaladsl.model.headers.{ContentDispositionType, ContentDispositionTypes}
import akka.http.scaladsl.model.headers.{Location, RawHeader}
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, FromResponseUnmarshaller, Unmarshaller}
import akka.stream.scaladsl.Source
import akka.util.ByteString
import io.bullet.borer.compat.akka.ByteStringProvider
import org.tresql.{Resources, Result, RowLike}

import scala.collection.immutable.{Seq => iSeq}
import scala.language.implicitConversions
import scala.language.reflectiveCalls
import scala.util.Try

trait Marshalling extends DtoMarshalling
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

  val dbBufferSize = 1024 * 32
  val dbDataFileMaxSize = MarshallingConfig.dbDataFileMaxSize

  protected def resultMaxFileSize(viewName: String): Long =
    MarshallingConfig.customDataFileMaxSizes.getOrElse(viewName, dbDataFileMaxSize)

  protected def source(src: Source[ByteString, _],
                       maxFileSize: Long,
                       cleanupFun: Option[Throwable] => Unit): Future[SerializedResult] = {
    RowSource.value(dbBufferSize, maxFileSize, src, cleanupFun)
  }

  protected def httpResponse(contentType: ContentType,
                             src: Source[ByteString, _],
                             maxFileSize: Long = dbBufferSize,
                             cleanupFun: Option[Throwable] => Unit): Future[HttpResponse] = {
    source(src, maxFileSize, cleanupFun).map {
      case CompleteResult(data) => HttpResponse(entity = HttpEntity.Strict(contentType, data))
      case IncompleteResultSource(sourceData) => HttpResponse(entity = HttpEntity.Chunked.fromData(contentType, sourceData))
    }.recover { case InsufficientStorageException(msg) =>
      HttpResponse(status = StatusCodes.InsufficientStorage,
        entity = HttpEntity.Strict(ContentTypes.`text/plain(UTF-8)`, ByteString(msg)))
    }
  }

  import RowSource._
  protected def rowWriterToResponseMarshaller[T](contentType: ContentType,
                                                 writerFun: (T, Writer) => RowWriter,
                                                 fileBufferMaxSize: T => Long = (_: T) => dbDataFileMaxSize,
                                                 cleanupFun: Option[Throwable] => Unit = null): ToResponseMarshaller[T] =
    Marshaller.combined(r => httpResponse(contentType, writerFun(r, _), fileBufferMaxSize(r), cleanupFun))

  protected def rowZipWriterToResponseMarshaller[T](contentType: ContentType,
                                                    writerFun: (T, ZipOutputStream) => RowWriter,
                                                    fileBufferMaxSize: T => Long = (_: T) => dbDataFileMaxSize,
                                                    cleanupFun: Option[Throwable] => Unit = null): ToResponseMarshaller[T] =
    Marshaller.combined(r => httpResponse(contentType, writerFun(r, _), fileBufferMaxSize(r), cleanupFun))

  implicit def toResponseListJsonMarshaller[T: JsonFormat]: ToResponseMarshaller[Iterator[T]] =
    rowWriterToResponseMarshaller(`application/json`, new app.JsonRowWriter(_, _))
}

trait DtoMarshalling extends AppMarshalling with Loggable { this: AppServiceBase[_] with Execution =>

  import jsonConverter._
  import AppMetadata._

  import app.qe

  implicit def dtoUnmarshaller[T <: app.Dto](implicit jsonUnmarshaller: FromEntityUnmarshaller[JsValue], m: Manifest[T]): FromEntityUnmarshaller[T] =
    jsonUnmarshaller.map(js => m.runtimeClass.getConstructor().newInstance().asInstanceOf[T].fill(js.asJsObject()))

  implicit def dtoListUnmarshaller[T <: app.Dto](implicit jsonUnmarshaller: FromEntityUnmarshaller[JsValue], m: Manifest[T]): FromEntityUnmarshaller[List[T]] =
    jsonUnmarshaller.map(responseJson =>
      responseJson.convertTo[List[Any]].map { csJs =>
        m.runtimeClass.getConstructor().newInstance().asInstanceOf[T].fill(csJs.asInstanceOf[Map[String, Any]].toJson.asJsObject())
      }
    )

  implicit val dtoMarshaller: ToEntityMarshaller[app.Dto] = Marshaller.withFixedContentType(`application/json`) {
    dto => HttpEntity.Strict(`application/json`, ByteString(dto.toMap.toJson.compactPrint))
  }
}

trait QuereaseResultMarshalling { this:
    BasicJsonMarshalling    with
    DtoMarshalling          with
    Execution               with
    AppServiceBase[_] =>

  import app.qe
  import app.qe.ListJsonFormat
  import app.qe.QuereaseIdResultJsonFormat
  import ResultEncoder.EncoderFactory
  implicit val toEntityQuereaseMapResultMarshaller:         ToEntityMarshaller  [MapResult]      =
    Marshaller.combined(_.result)
  implicit val toEntityQuereasePojoResultMarshaller:        ToEntityMarshaller  [PojoResult]     =
    Marshaller.combined(_.result.toMap)
  implicit val toEntityQuereaseListResultMarshaller:        ToEntityMarshaller  [ListResult]     =
    Marshaller.combined(_.result.toJson)
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
  implicit val toEntityQuereaseDeleteResultMarshaller:      ToEntityMarshaller[QuereaseDeleteResult] =
    Marshaller.combined(_.count.toString)

  def toEntitySerializedResultMarshaller(
    contentType: ContentType,
    createEncoder: EncoderFactory,
  ): ToEntityMarshaller[SerializedResult] = {
    Marshaller.withFixedContentType(contentType) {
      case CompleteResult(bytes) =>
        HttpEntity.Strict(contentType, BorerNestedArraysTransformer.transform(bytes, createEncoder))
      case IncompleteResultSource(src)  =>
        HttpEntity.Chunked.fromData(contentType, src.via(BorerNestedArraysTransformer.flow(createEncoder)))
    }
  }

  def createJsonEncoderFactory(viewName: String, isCollection: Boolean): EncoderFactory =
    JsonOutput(_, isCollection, viewName, qe.nameToViewDef)
  def createCsvEncoderFactory(viewName: String): EncoderFactory =
    os => new CsvOutput(new OutputStreamWriter(os, "UTF-8"), qe.viewNameToLabels(viewName))
  def createOdsEncoderFactory(viewName: String): EncoderFactory =
    os => new OdsOutput(new ZipOutputStream(os), qe.viewNameToLabels(viewName))
  def createXlsXmlEncoderFactory(viewName: String): EncoderFactory =
    os => new XlsXmlOutput(new OutputStreamWriter(os, "UTF-8"), qe.viewNameToLabels(viewName))

  def toEntityQuereaseSerializedResultMarshaller(actionName: String, viewName: String): ToEntityMarshaller[QuereaseSerializedResult] = {
    implicit val formats_marshaller: ToEntityMarshaller[SerializedResult] =
      Marshaller.oneOf(
        toEntitySerializedResultMarshaller(`application/json`,                createJsonEncoderFactory(viewName, actionName == AppMetadata.Action.List)),
        toEntitySerializedResultMarshaller(ContentTypes.`text/csv(UTF-8)`,    createCsvEncoderFactory(viewName)),
        toEntitySerializedResultMarshaller(`application/vnd.oasis.opendocument.spreadsheet`,
                                                                              createOdsEncoderFactory(viewName)),
        toEntitySerializedResultMarshaller(`application/vnd.ms-excel`,        createXlsXmlEncoderFactory(viewName)),
      )
    Marshaller.combined(_.result)
  }

  import org.wabase.{QuereaseSerializedResult => QuereaseSerRes}
  implicit def toResponseWabaseResultMarshaller(implicit ec: ExecutionContext): ToResponseMarshaller[app.WabaseResult] =
    Marshaller { implicit ec => wr => wr.result match {
      case tq: TresqlResult   => sys.error("TresqlResult must be serialized before marshalling.")//(toResponseQuereaseTresqlResultMarshaller:   ToResponseMarshaller[TresqlResult]  )(tq)
      case rr: TresqlSingleRowResult => sys.error("TresqlSingleRowResult must be serialized before marshalling.")
      case mp: MapResult      => (toEntityQuereaseMapResultMarshaller:        ToResponseMarshaller[MapResult]     )(mp)
      case pj: PojoResult     => (toEntityQuereasePojoResultMarshaller:       ToResponseMarshaller[PojoResult]    )(pj)
      case ls: ListResult     => (toEntityQuereaseListResultMarshaller:       ToResponseMarshaller[ListResult]    )(ls)
      case it: IteratorResult => sys.error("IteratorResult must be serialized before marshalling.")//(toEntityQuereaseIteratorResultMarshaller:   ToResponseMarshaller[IteratorResult])(it)
      case op: OptionResult   => (toResponseQuereaseOptionResultMarshaller:   ToResponseMarshaller[OptionResult]  )(op)
      case nr: NumberResult   => (toEntityQuereaseNumberResultMarshaller:     ToResponseMarshaller[NumberResult]  )(nr)
      case cd: CodeResult     => (toEntityQuereaseCodeResultMarshaller:       ToResponseMarshaller[CodeResult]    )(cd)
      case id: IdResult       => (toEntityQuereaseIdResultMarshaller:         ToResponseMarshaller[IdResult]      )(id)
      case rd: RedirectResult => (toResponseQuereaseRedirectResultMarshaller: ToResponseMarshaller[RedirectResult])(rd)
      case no: NoResult.type  => (toEntityQuereaseNoResultMarshaller:         ToResponseMarshaller[NoResult.type] )(no)
      case cr: QuereaseSerRes => (toEntityQuereaseSerializedResultMarshaller(
        AppMetadata.Action.List,
        wr.ctx.viewName): ToResponseMarshaller[QuereaseSerRes])(cr)
      case dr: QuereaseDeleteResult =>
        (toEntityQuereaseDeleteResultMarshaller: ToResponseMarshaller[QuereaseDeleteResult])(dr)
      case r: QuereaseResultWithCleanup =>
        sys.error(s"QuereaseResult marshaller for class ${r.getClass.getName} not implemented")
    }}

  implicit val toResponseQuereaseIteratorMarshaller: ToResponseMarshaller[qe.QuereaseIteratorResult[app.Dto]] = {
    def marsh(viewName: String)(implicit ec: ExecutionContext): ToResponseMarshaller[qe.QuereaseIteratorResult[app.Dto]] =
      Marshaller.combined { qir: qe.QuereaseIteratorResult[app.Dto] =>
        app.serializeResult(app.SerializationBufferSize, app.viewSerializationBufferMaxFileSize(viewName),
          DtoDataSerializer.source(() => qir))
      } (GenericMarshallers.futureMarshaller(toEntityQuereaseSerializedResultMarshaller(AppMetadata.Action.List, viewName)))

    Marshaller { ec => res => marsh(res.view.name)(ec)(res) }
  }

  implicit def toResponseTresqlResultMarshaller(implicit res: Resources): ToEntityMarshaller[RowLike] =
    Marshaller { _ => tresqlResult =>
      val resType = tresqlResult match {
        case _: Result[_] => AppMetadata.Action.List
        case _ => AppMetadata.Action.Get
      }
      val sr = app.serializeResult(app.SerializationBufferSize, app.SerializationBufferMaxFileSize,
        TresqlResultSerializer.source(() => tresqlResult), app.dbAccess.closeResources(res, _))
      GenericMarshallers
        .futureMarshaller(toEntityQuereaseSerializedResultMarshaller(resType, null))(sr)
    }
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
