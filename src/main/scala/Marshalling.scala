package org.wabase

import akka.NotUsed
import akka.http.scaladsl.marshalling._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.MediaTypes._
import akka.http.scaladsl.model.headers.ContentDispositionTypes.attachment
import akka.http.scaladsl.util.FastFuture

import java.net.URLEncoder
import java.text.Normalizer
import scala.concurrent.{ExecutionContext, Future}
import akka.http.scaladsl.model.headers.{ContentDispositionType, ContentDispositionTypes, Location, RawHeader, `Content-Disposition`}
import akka.http.scaladsl.server.RouteResult.{Complete, Rejected}
import akka.http.scaladsl.server.directives.{FileAndResourceDirectives}
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, FromResponseUnmarshaller, Unmarshaller}
import akka.stream.scaladsl.{Flow, Source}
import akka.util.ByteString
import io.bullet.borer.compat.akka.ByteStringProvider
import org.mojoz.metadata.ViewDef
import org.mojoz.querease.QuereaseIteratorResult
import org.tresql.{Resources, Result, RowLike}

import scala.collection.immutable.Seq
import scala.language.implicitConversions
import scala.language.reflectiveCalls

trait Marshalling extends
       BasicJsonMarshalling
  with BasicMarshalling
  with QuereaseMarshalling
  with DtoMarshalling
  { this: AppProvider[_] with JsonConverterProvider with Execution => }

trait BasicJsonMarshalling extends akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport with BasicMarshalling {
  this: JsonConverterProvider =>

  import spray.json._
  import DefaultJsonProtocol._
  import jsonConverter._

  implicit val mapMarshaller: ToEntityMarshaller[Map[String, Any]] = Marshaller.combined(_.toJson)

  implicit val listOfMapsMarshaller: ToEntityMarshaller[List[Map[String, Any]]] = Marshaller.combined(_.toJson)

  implicit def futureMapMarshaller: ToEntityMarshaller[Future[Map[String, Any]]] =
    combinedWithEC(ec => mapF => mapF.map(_.toJson)(ec))

  def mapUnmarshaller(implicit jsonUnmarshaller: FromEntityUnmarshaller[JsValue]): FromEntityUnmarshaller[Map[String, Any]] =
    jsonUnmarshaller.map(_.convertTo[Map[String, Any]])

  implicit def jsObjectUnmarshaller(
    implicit jsonUnmarshaller: FromEntityUnmarshaller[JsValue]
  ): Unmarshaller[HttpEntity, JsObject] = jsonUnmarshaller.map(_.asJsObject)
}

trait OptionMarshalling {
  implicit def fromResponseOptionUnmarshaller[T](implicit unm: FromResponseUnmarshaller[T]): FromResponseUnmarshaller[Option[T]] =
    Unmarshaller.withMaterializer{implicit ec => implicit mat => entity =>
      if (entity.status == StatusCodes.NotFound || entity.status == StatusCodes.NoContent) Future.successful(None) else unm(entity).map(r => Option(r))
    }
  implicit def toResponseOptionMarshaller[A](implicit m: ToResponseMarshaller[A]): ToResponseMarshaller[Option[A]] = Marshaller { implicit ec => {
    case Some(value) => m(value)
    case None => FastFuture.successful(Marshalling.Opaque(() => HttpResponse(StatusCodes.NotFound)) :: Nil)
  }}
}

trait BasicMarshalling extends OptionMarshalling {

  // why this is not the method in Marshaller, like it has composeWithEC and wrapWithEC ???
  def combinedWithEC[A, B, C](marshal: ExecutionContext => A => B)(implicit m2: Marshaller[B, C]): Marshaller[A, C] =
    Marshaller[A, C] { ec => a => m2.composeWithEC(marshal).apply(a)(ec) }

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
  implicit val HeadersUnmarshaller: FromResponseUnmarshaller[Seq[HttpHeader]] = Unmarshaller{implicit ec => resp => Future.successful(resp.headers)}
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

trait DtoMarshalling extends QuereaseMarshalling { this: AppProvider[_] with Execution with OptionMarshalling =>
  import app.qe
  implicit def dtoUnmarshaller[T <: Dto](implicit m: Manifest[T]): FromEntityUnmarshaller[T] =
    toMapUnmarshallerForView(app.qe.viewDefFromMf[T].name).map {
      m.runtimeClass.getConstructor().newInstance().asInstanceOf[T].fill
    }

  implicit def dtoSeqUnmarshaller[T <: Dto](implicit m: Manifest[T]): FromEntityUnmarshaller[Seq[T]] =
    toSeqOfMapsUnmarshallerForView(app.qe.viewDefFromMf[T].name).map { _.map {
      m.runtimeClass.getConstructor().newInstance().asInstanceOf[T].fill
    }}

  implicit val dtoForViewMarshaller: ToEntityMarshaller[(Dto, String)] =
    Marshaller.combined { case (dto, viewName) => (dto.toMap, viewName, null) }
  implicit val dtoMarshaller: ToEntityMarshaller[Dto] =
    Marshaller.combined { (dto: Dto) => (dto, app.qe.classToViewNameMap.get(dto.getClass).orNull) }
  implicit val dtoSeqForViewMarshaller: ToEntityMarshaller[(Seq[Dto], String)] =
    Marshaller.combined { case (seqOfDto, viewName) => (seqOfDto.map(_.toMap), viewName, null) }
  implicit val dtoSeqMarshaller: ToEntityMarshaller[Seq[Dto]] =
    Marshaller.combined { dtoSeq =>
      (dtoSeq, dtoSeq.find(_ != null).map(_.getClass).flatMap(app.qe.classToViewNameMap.get).orNull)
    }
}

trait QuereaseMarshalling extends QuereaseResultMarshalling { this: AppProvider[_] with Execution with OptionMarshalling =>
  import app.qe
  implicit val mapForViewMarshaller: ToEntityMarshaller[(Map[String, Any], String, ResultRenderer.ResultFilter)] = {
    def marsh(viewName: String, resFilter: ResultRenderer.ResultFilter)(implicit ec: ExecutionContext): ToEntityMarshaller[Map[String, Any]] =
      Marshaller.combined { (map: Map[String, Any]) =>
        // TODO transcode directly
        app.serializeResult(app.SerializationBufferSize, app.viewSerializationBufferMaxFileSize(viewName),
          DataSerializer.source(() => Seq(map).iterator)).map(_.head)
          .map(QuereaseSerializedResult(_, resFilter, isCollection = false))
      } (GenericMarshallers.futureMarshaller(toEntityQuereaseSerializedResultMarshaller(viewName, null)))
    Marshaller { ec => mapAndView => marsh(mapAndView._2, mapAndView._3)(ec)(mapAndView._1) }
  }

  implicit val seqOfMapsForViewMarshaller: ToEntityMarshaller[(Seq[Map[String, Any]], String, ResultRenderer.ResultFilter)] = {
    def marsh(viewName: String, resFilter: ResultRenderer.ResultFilter)(implicit ec: ExecutionContext): ToEntityMarshaller[Seq[Map[String, Any]]] =
      Marshaller.combined { (seqOfMaps: Seq[Map[String, Any]]) =>
        // TODO transcode directly
        app.serializeResult(app.SerializationBufferSize, app.viewSerializationBufferMaxFileSize(viewName),
          DataSerializer.source(() => seqOfMaps.iterator)).map(_.head)
          .map(QuereaseSerializedResult(_, resFilter, isCollection = true))
      } (GenericMarshallers.futureMarshaller(toEntityQuereaseSerializedResultMarshaller(viewName, null)))
    Marshaller { ec => seqOfMapsAndView => marsh(seqOfMapsAndView._2, seqOfMapsAndView._3)(ec)(seqOfMapsAndView._1) }
  }

  def toMapUnmarshallerForView(viewName: String): FromEntityUnmarshaller[Map[String, Any]] =
    Unmarshaller.byteStringUnmarshaller map { bytes =>
      app.qe.cborOrJsonDecoder.decodeToMap(bytes, viewName)(app.qe.viewNameToMapZero)
    }
  def toSeqOfMapsUnmarshallerForView(viewName: String): FromEntityUnmarshaller[Seq[Map[String, Any]]] =
    Unmarshaller.byteStringUnmarshaller map { bytes =>
      app.qe.cborOrJsonDecoder.decodeToSeqOfMaps(bytes, viewName)(app.qe.viewNameToMapZero)
    }
}

trait QuereaseResultMarshalling { this: AppProvider[_] with Execution with QuereaseMarshalling with OptionMarshalling =>
  import app.qe
  import ResultEncoder.EncoderFactory
  implicit def toEntityQuereaseMapResultMarshaller (viewName: String,
                                                    resFilter: ResultRenderer.ResultFilter):  ToEntityMarshaller[MapResult]  =
    Marshaller.combined((mr:  MapResult) => (mr.result, viewName, resFilter))
  implicit val toEntityQuereaseLongResultMarshaller:      ToEntityMarshaller  [LongResult]   =
    Marshaller.combined("" + _.value)
  implicit val toEntityQuereaseStringResultMarshaller:    ToEntityMarshaller  [StringResult]     =
    Marshaller.combined(_.value)
  implicit val toEntityQuereaseNumberResultMarshaller:    ToEntityMarshaller  [NumberResult]     =
    Marshaller.combined(_.value.toString)
  implicit val toEntityQuereaseIdResultMarshaller:        ToEntityMarshaller  [IdResult]       =
    Marshaller.combined(_.toString)
  implicit def toResponseQuereaseKeyResultMarshaller:     ToResponseMarshaller[KeyResult]      =
    Marshaller.combined((kr: KeyResult) =>
      StatusResult(StatusCodes.SeeOther.intValue, RedirectStatus(TresqlUri.Uri(s"/data/${kr.viewName}", kr.key)))
    )
  implicit val toResponseQuereaseStatusResultMarshaller:  ToResponseMarshaller[StatusResult] = {
    Marshaller.opaque { sr =>
      val status: StatusCode = sr.code
      sr.value match {
        case RedirectStatus(value) =>
          require(value != null, s"Error marshalling redirect status result - no uri.")
          HttpResponse(status, headers = Seq(Location(app.qe.tresqlUri.uri(value))))
        case StringStatus(value) =>
          HttpResponse(status, entity = HttpEntity.Strict(ContentTypes.`text/plain(UTF-8)`, ByteString(value)))
        case null =>
          HttpResponse(status, entity = HttpEntity.Empty)
      }
    }
  }
  implicit val toEntityQuereaseNoResultMarshaller:          ToEntityMarshaller  [NoResult.type]  =
    Marshaller.combined(_ => "")
  implicit val toEntityQuereaseDeleteResultMarshaller:      ToEntityMarshaller[QuereaseDeleteResult] =
    Marshaller.combined(_.count.toString)
  implicit val toResponseFileResultMarshaller:              ToResponseMarshaller[FileResult] = Marshaller.combined {
    fr => app.qe.fileHttpEntity(fr).map { ent =>
      HttpResponse(status = StatusCodes.OK, entity = ent)
    }.getOrElse(HttpResponse(status = StatusCodes.NotFound))
  }
  implicit val toResponseResourceResultMarshaller:          ToResponseMarshaller[ResourceResult] = Marshaller.combined {
    rr => FileAndResourceDirectives
      .getFromResource(rr.resource, rr.contentType)(rr.httpCtx)
      .map {
        case Complete(response) => response
        case _: Rejected => HttpResponse(status = StatusCodes.NotFound)
      }
  }
  implicit val toResponseTemplateResultMarshaller:          ToResponseMarshaller[TemplateResult] =
    Marshaller.combined {
      case StringTemplateResult(content) =>
        HttpResponse(status = StatusCodes.OK, entity = HttpEntity(content))
      case FileTemplateResult(fileName, contentType, content) =>
        ContentType.parse(contentType).fold(
          err => HttpResponse(
            status = StatusCodes.InternalServerError,
            entity = HttpEntity(err.map(_.formatPretty).mkString("\n"))
          ),
          ct => HttpResponse(
            status = StatusCodes.OK,
            headers = List(`Content-Disposition`(attachment, Map("filename" -> fileName))),
            entity = HttpEntity(ct, content)
          )
        )
    }
  implicit val toResponseHttpResultMarshaller:              ToResponseMarshaller[HttpResult] =
    Marshaller.combined(_.response)

  implicit val toEntityConfResultMarshaller:                ToEntityMarshaller[ConfResult] =
    Marshaller.combined { cr =>
      import ResultEncoder._
      implicit lazy val enc: JsValueEncoderPF = JsonEncoder.extendableJsValueEncoderPF(enc)(app.qe.jsonValueEncoder)
      HttpEntity.Strict(`application/json`, ByteString(encodeJsValue(cr.result)))
    }

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

  private def getResultFilter(viewName: String,
                              filter1: ResultRenderer.ResultFilter,
                              filter2: ResultRenderer.ResultFilter) = {
    if (viewName == null && filter1 == null && filter2 == null) null
    else if (filter1 == null && filter2 == null) new ResultRenderer.ViewFieldFilter(viewName, app.qe.nameToViewDef)
    else if (filter1 == null) filter2
    else if (filter2 == null) filter1
    else new ResultRenderer.IntersectionFilter(filter1, filter2)
  }

  def toResponseCompatibleResultMarshaller(wr: app.WabaseResult): ToResponseMarshaller[CompatibleResult] = {
    Marshaller { implicit ec => cr =>
      val fil = getResultFilter(wr.ctx.viewName, wr.ctx.resultFilter, cr.resultFilter)
      val ctx = wr.ctx.withResultFilter(fil)
      (toResponseWabaseResultMarshaller: ToResponseMarshaller[app.WabaseResult])(
        wr.copy(ctx = ctx, result = cr.result))
    }
  }

  def toEntityQuereaseSerializedResultMarshaller(viewName: String,
                                                 resultFilter: ResultRenderer.ResultFilter): ToEntityMarshaller[QuereaseSerializedResult] =
    Marshaller { implicit ec => sr =>
      implicit val formats_marshaller: ToEntityMarshaller[SerializedResult] = {
        val marshallers = qe.resultRenderers.renderers.map {
          case (contentType, encCreator) =>
            val vd = qe.nameToViewDef(viewName)
            val fil = getResultFilter(viewName, resultFilter, sr.resultFilter)
            toEntitySerializedResultMarshaller(contentType, encCreator(sr.isCollection, fil, vd))
        }.toSeq
        Marshaller.oneOf(marshallers: _*)
      }
      formats_marshaller(sr.result)
    }

  import org.wabase.{QuereaseSerializedResult => QuereaseSerRes}
  import org.wabase.{QuereaseDeleteResult     => QuereaseDelRes}
  import org.wabase.{TresqlSingleRowResult    => TresqlSingleRr}
  implicit def toResponseWabaseResultMarshaller(implicit ec: ExecutionContext): ToResponseMarshaller[app.WabaseResult] =
    Marshaller { implicit ec => wr => wr.result match {
      case sr: QuereaseSerRes =>
        (toEntityQuereaseSerializedResultMarshaller (
          wr.ctx.viewName, wr.ctx.resultFilter):                                                       ToResponseMarshaller[QuereaseSerRes])(sr)
      case mp: MapResult      =>
        (toEntityQuereaseMapResultMarshaller        (
          wr.ctx.viewName, wr.ctx.resultFilter):                                                       ToResponseMarshaller[MapResult]     )(mp)
      case nr: LongResult     => (toEntityQuereaseLongResultMarshaller:       ToResponseMarshaller[LongResult]    )(nr)
      case sr: StringResult   => (toEntityQuereaseStringResultMarshaller:     ToResponseMarshaller[StringResult]  )(sr)
      case nr: NumberResult   => (toEntityQuereaseNumberResultMarshaller:     ToResponseMarshaller[NumberResult]  )(nr)
      case id: IdResult       => (toEntityQuereaseIdResultMarshaller:         ToResponseMarshaller[IdResult]      )(id)
      case kr: KeyResult      => (toResponseQuereaseKeyResultMarshaller:      ToResponseMarshaller[KeyResult]     )(kr)
      case sr: StatusResult   => (toResponseQuereaseStatusResultMarshaller:   ToResponseMarshaller[StatusResult]  )(sr)
      case no: NoResult.type  => (toEntityQuereaseNoResultMarshaller:         ToResponseMarshaller[NoResult.type] )(no)
      case dr: QuereaseDelRes => (toEntityQuereaseDeleteResultMarshaller:     ToResponseMarshaller[QuereaseDelRes])(dr)
      case fr: FileResult     => (toResponseFileResultMarshaller:             ToResponseMarshaller[FileResult]    )(fr)
      case rr: ResourceResult => (toResponseResourceResultMarshaller:         ToResponseMarshaller[ResourceResult])(rr)
      case tr: TemplateResult => (toResponseTemplateResultMarshaller:         ToResponseMarshaller[TemplateResult])(tr)
      case hr: HttpResult     => (toResponseHttpResultMarshaller:             ToResponseMarshaller[HttpResult]    )(hr)
      case cr: CompatibleResult => (toResponseCompatibleResultMarshaller(wr): ToResponseMarshaller[CompatibleResult])(cr)
      case cr: ConfResult     => (toEntityConfResultMarshaller:               ToResponseMarshaller[ConfResult]    )(cr)
      case tq: TresqlResult   => sys.error("TresqlResult must be serialized before marshalling.")
      case rr: TresqlSingleRr => sys.error("TresqlSingleRowResult must be serialized before marshalling.")
      case it: IteratorResult => sys.error("IteratorResult must be serialized before marshalling.")
      case fr: FileInfoResult => sys.error("File info result marshalling not supported")
      case db: DbResult       => sys.error("Db result cannot be marshalled directly, unwrap inner result and try marshalling.")
      case r: QuereaseResultWithCleanup =>
        sys.error(s"QuereaseResult marshaller for class ${r.getClass.getName} not implemented")
    }}

  implicit val toResponseQuereaseIteratorMarshaller: ToResponseMarshaller[QuereaseIteratorResult[Dto]] = {
    def marsh(viewName: String)(implicit ec: ExecutionContext): ToResponseMarshaller[QuereaseIteratorResult[Dto]] =
      Marshaller.combined { (qir: QuereaseIteratorResult[Dto]) =>
        app.serializeResult(app.SerializationBufferSize, app.viewSerializationBufferMaxFileSize(viewName),
          DataSerializer.source(() => qir.map(_.toMap))).map(_.head)
          .map(QuereaseSerializedResult(_, null, isCollection = true))
      } (GenericMarshallers.futureMarshaller(toEntityQuereaseSerializedResultMarshaller(viewName, null)))

    Marshaller { ec => res => marsh(res.view.name)(ec)(res) }
  }

  implicit def toResponseTresqlResultMarshaller(implicit res: Resources): ToEntityMarshaller[RowLike] =
    Marshaller { _ => tresqlResult =>
      val sr = app.serializeResult(app.SerializationBufferSize, app.SerializationBufferMaxFileSize,
        tresqlResult match {
          case result: Result[_] => TresqlResultSerializer.source(() => result)
          case row:    RowLike   => TresqlResultSerializer.rowSource(() => row)
        }, app.dbAccess.closeResources(res, false, _))
        .map(_.head)
        .map(QuereaseSerializedResult(_, null, tresqlResult.isInstanceOf[Result[_]]))
      GenericMarshallers
        .futureMarshaller(toEntityQuereaseSerializedResultMarshaller(null, null))(sr)
    }

  def serializedResultToJsonFlow(isCollection: Boolean,
                                 resultFilter: ResultRenderer.ResultFilter): Flow[ByteString, ByteString, NotUsed] =
    BorerNestedArraysTransformer.flow(ResultRenderers.createJsonEncoderFactory(isCollection, resultFilter, null))
  def serializedResultToCborFlow(isCollection: Boolean,
                                 resultFilter: ResultRenderer.ResultFilter): Flow[ByteString, ByteString, NotUsed] =
    BorerNestedArraysTransformer.flow(ResultRenderers.createCborEncoderFactory(isCollection, resultFilter, null))
  def serializedResultToCsvFlow(resultFilter: ResultRenderer.ResultFilter,
                                viewDef: ViewDef): Flow[ByteString, ByteString, NotUsed] =
    BorerNestedArraysTransformer.flow(ResultRenderers.createCsvEncoderFactory(true, resultFilter, viewDef))
  def serializedResultToOdsFlow(resultFilter: ResultRenderer.ResultFilter,
                                viewDef: ViewDef): Flow[ByteString, ByteString, NotUsed] =
    BorerNestedArraysTransformer.flow(ResultRenderers.createOdsEncoderFactory(true, resultFilter, viewDef))
  def serializedResultToXlsXmlFlow(resultFilter: ResultRenderer.ResultFilter,
                                   viewDef: ViewDef): Flow[ByteString, ByteString, NotUsed] =
    BorerNestedArraysTransformer.flow(ResultRenderers.createXlsXmlEncoderFactory(true, resultFilter, viewDef))
}

object MarshallingConfig extends AppBase.AppConfig with Loggable {
  lazy val dbDataFileMaxSize: Long = appConfig.getBytes("db-data-file-max-size")
  import scala.jdk.CollectionConverters._
  lazy val customDataFileMaxSizes: Map[String, Long] = {
    val sc   = appConfig.getConfig("db-data-file-max-sizes")
    val vals = sc.entrySet.asScala.map(e => e.getKey -> sc.getBytes(e.getKey).toLong).toMap
    logger.debug(s"Custom db data max file sizes: $vals")
    vals
  }
}
