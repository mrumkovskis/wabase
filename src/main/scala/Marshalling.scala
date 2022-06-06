package org.wabase

import akka.http.scaladsl.util.FastFuture
import akka.http.scaladsl._
import model._
import MediaTypes._
import marshalling._

import java.io.OutputStreamWriter
import java.net.URLEncoder
import java.text.Normalizer
import java.util.zip.ZipOutputStream
import scala.concurrent.{ExecutionContext, Future}
import akka.http.scaladsl.model.headers.{ContentDispositionType, ContentDispositionTypes}
import akka.http.scaladsl.model.headers.{Location, RawHeader}
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, FromResponseUnmarshaller, Unmarshaller}
import akka.stream.scaladsl.Source
import akka.util.ByteString
import io.bullet.borer.compat.akka.ByteStringProvider
import org.tresql.{Resources, Result, RowLike}

import scala.collection.immutable.Seq
import scala.language.implicitConversions
import scala.language.reflectiveCalls
import scala.util.Try

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

  // implicit def mapUnmarshaller(implicit jsonUnmarshaller: FromEntityUnmarshaller[JsValue]): FromEntityUnmarshaller[Map[String, Any]] =
  //   jsonUnmarshaller.map(_.convertTo[Map[String, Any]])

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

trait DtoMarshalling extends QuereaseMarshalling { this: AppProvider[_] with Execution =>
  import app.qe
  implicit def dtoUnmarshaller[T <: app.Dto](implicit m: Manifest[T]): FromEntityUnmarshaller[T] =
    toMapUnmarshallerForView(app.qe.viewDef[T].name).map {
      m.runtimeClass.getConstructor().newInstance().asInstanceOf[T].fill
    }

  implicit def dtoSeqUnmarshaller[T <: app.Dto](implicit m: Manifest[T]): FromEntityUnmarshaller[Seq[T]] =
    toSeqOfMapsUnmarshallerForView(app.qe.viewDef[T].name).map { _.map {
      m.runtimeClass.getConstructor().newInstance().asInstanceOf[T].fill
    }}

  implicit val dtoForViewMarshaller: ToEntityMarshaller[(app.Dto, String)] =
    Marshaller.combined { case (dto, viewName) => (dto.toMap, viewName) }
  implicit val dtoMarshaller: ToEntityMarshaller[app.Dto] =
    Marshaller.combined { dto: app.Dto => (dto, app.qe.classToViewNameMap.get(dto.getClass).orNull) }
  implicit val dtoSeqForViewMarshaller: ToEntityMarshaller[(Seq[app.Dto], String)] =
    Marshaller.combined { case (seqOfDto, viewName) => (seqOfDto.map(_.toMap), viewName) }
  implicit val dtoSeqMarshaller: ToEntityMarshaller[Seq[app.Dto]] =
    Marshaller.combined { dtoSeq =>
      (dtoSeq, dtoSeq.find(_ != null).map(_.getClass).flatMap(app.qe.classToViewNameMap.get).orNull)
    }
}

trait QuereaseMarshalling extends QuereaseResultMarshalling { this: AppProvider[_] with Execution =>
  import app.qe
  implicit val mapForViewMarshaller: ToEntityMarshaller[(Map[String, Any], String)] = {
    def marsh(viewName: String)(implicit ec: ExecutionContext): ToEntityMarshaller[Map[String, Any]] =
      Marshaller.combined { map: Map[String, Any] =>
        // TODO transcode directly
        app.serializeResult(app.SerializationBufferSize, app.viewSerializationBufferMaxFileSize(viewName),
          DtoDataSerializer.source(() => Seq(map).iterator), isCollection = false)
      } (GenericMarshallers.futureMarshaller(toEntityQuereaseSerializedResultMarshaller(viewName)))
    Marshaller { ec => mapAndView => marsh(mapAndView._2)(ec)(mapAndView._1) }
  }

  implicit val seqOfMapsForViewMarshaller: ToEntityMarshaller[(Seq[Map[String, Any]], String)] = {
    def marsh(viewName: String)(implicit ec: ExecutionContext): ToEntityMarshaller[Seq[Map[String, Any]]] =
      Marshaller.combined { seqOfMaps: Seq[Map[String, Any]] =>
        // TODO transcode directly
        app.serializeResult(app.SerializationBufferSize, app.viewSerializationBufferMaxFileSize(viewName),
          DtoDataSerializer.source(() => seqOfMaps.iterator), isCollection = true)
      } (GenericMarshallers.futureMarshaller(toEntityQuereaseSerializedResultMarshaller(viewName)))
    Marshaller { ec => seqOfMapsAndView => marsh(seqOfMapsAndView._2)(ec)(seqOfMapsAndView._1) }
  }

  val cborOrJsonDecoder = new CborOrJsonDecoder(app.qe.typeDefs, app.qe.nameToViewDef)
  def toMapUnmarshallerForView(viewName: String): FromEntityUnmarshaller[Map[String, Any]] =
    Unmarshaller.byteStringUnmarshaller map { bytes =>
      cborOrJsonDecoder.decodeToMap(bytes, viewName)(app.qe.viewNameToMapZero)
    }
  def toSeqOfMapsUnmarshallerForView(viewName: String): FromEntityUnmarshaller[Seq[Map[String, Any]]] =
    Unmarshaller.byteStringUnmarshaller map { bytes =>
      cborOrJsonDecoder.decodeToSeqOfMaps(bytes, viewName)(app.qe.viewNameToMapZero)
    }
}

trait QuereaseResultMarshalling { this: AppProvider[_] with Execution with QuereaseMarshalling =>
  import app.qe
  import ResultEncoder.EncoderFactory
  implicit def toEntityQuereaseMapResultMarshaller (viewName: String):  ToEntityMarshaller[MapResult]  =
    Marshaller.combined((mr:  MapResult) => (mr.result, viewName))
  implicit def toEntityQuereasePojoResultMarshaller(viewName: String):  ToEntityMarshaller[PojoResult] =
    Marshaller.combined((pr: PojoResult) => (pr.result.toMap, viewName))
  implicit def toEntityQuereaseListResultMarshaller(viewName: String):  ToEntityMarshaller[ListResult] =
    Marshaller.combined((lr: ListResult) => (lr.result.map(_.toMap), viewName))
  implicit def toResponseQuereaseOptionResultMarshaller(viewName: String):  ToResponseMarshaller[OptionResult] =
    Marshaller.combined(_.result.map(dto => (dto.toMap, viewName)))
  implicit val toEntityQuereaseNumberResultMarshaller:      ToEntityMarshaller  [NumberResult]   =
    Marshaller.combined("" + _.id)
  implicit val toEntityQuereaseCodeResultMarshaller:        ToEntityMarshaller  [CodeResult]     =
    Marshaller.combined(_.code)
  implicit val toEntityQuereaseIdResultMarshaller:          ToEntityMarshaller  [IdResult]       =
    Marshaller.combined(_.toString)
  implicit val toResponseQuereaseStatusResultMarshaller:    ToResponseMarshaller[StatusResult] =
    Marshaller.opaque { sr =>
      val status: StatusCode = sr.code
      if (status.isRedirection()) {
        import akka.http.scaladsl.model.Uri._
        require(sr.value != null, s"Error marshalling redirect status result - no uri.")
        val uri: Uri = sr.value
        val path = sr.key.foldLeft(uri.path){ (p, k) => p / String.valueOf(k) }
        val nonNullParams = sr.params.map { case (k, v) => (k, if (v == null) "" else v) }
        HttpResponse(status, headers = Seq(Location(uri.withPath(path).withQuery(Query(nonNullParams)))))
      } else {
        val ent =
          if (sr.value == null) HttpEntity.Empty
          else HttpEntity.Strict(ContentTypes.`text/plain(UTF-8)`, ByteString(sr.value))
        HttpResponse(status, entity = ent)
      }
    }
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
    JsonResultRenderer(_, isCollection, viewName, qe.nameToViewDef)
  def createCborEncoderFactory(viewName: String, isCollection: Boolean): EncoderFactory =
    CborResultRenderer(_, isCollection, viewName, qe.nameToViewDef)
  def createCsvEncoderFactory(viewName: String): EncoderFactory =
    os => new FlatTableResultRenderer(new CsvResultRenderer(new OutputStreamWriter(os, "UTF-8")),
      viewName, qe.nameToViewDef)
  def createOdsEncoderFactory(viewName: String): EncoderFactory =
    os => new FlatTableResultRenderer(new OdsResultRenderer(new ZipOutputStream(os)),
      viewName, qe.nameToViewDef)
  def createXlsXmlEncoderFactory(viewName: String): EncoderFactory =
    os => new FlatTableResultRenderer(new XlsXmlResultRenderer(new OutputStreamWriter(os, "UTF-8")),
      viewName, qe.nameToViewDef)

  def toEntityQuereaseSerializedResultMarshaller(viewName: String): ToEntityMarshaller[QuereaseSerializedResult] =
    Marshaller { implicit ec => sr =>
      implicit val formats_marshaller: ToEntityMarshaller[SerializedResult] =
        Marshaller.oneOf(
          toEntitySerializedResultMarshaller(`application/json`,                createJsonEncoderFactory(viewName, sr.isCollection)),
          toEntitySerializedResultMarshaller(`application/cbor`,                createCborEncoderFactory(viewName, sr.isCollection)),
          toEntitySerializedResultMarshaller(ContentTypes.`text/csv(UTF-8)`,    createCsvEncoderFactory(viewName)),
          toEntitySerializedResultMarshaller(`application/vnd.oasis.opendocument.spreadsheet`,
                                                                                createOdsEncoderFactory(viewName)),
          toEntitySerializedResultMarshaller(`application/vnd.ms-excel`,        createXlsXmlEncoderFactory(viewName)),
        )
      formats_marshaller(sr.result)
    }

  import org.wabase.{QuereaseSerializedResult => QuereaseSerRes}
  import org.wabase.{QuereaseDeleteResult     => QuereaseDelRes}
  import org.wabase.{TresqlSingleRowResult    => TresqlSingleRr}
  implicit def toResponseWabaseResultMarshaller(implicit ec: ExecutionContext): ToResponseMarshaller[app.WabaseResult] =
    Marshaller { implicit ec => wr => wr.result match {
      case sr: QuereaseSerRes =>
        (toEntityQuereaseSerializedResultMarshaller (wr.ctx.viewName):        ToResponseMarshaller[QuereaseSerRes])(sr)
      case op: OptionResult   =>
        (toResponseQuereaseOptionResultMarshaller   (wr.ctx.viewName):        ToResponseMarshaller[OptionResult]  )(op)
      case mp: MapResult      =>
        (toEntityQuereaseMapResultMarshaller        (wr.ctx.viewName):        ToResponseMarshaller[MapResult]     )(mp)
      case pj: PojoResult     =>
        (toEntityQuereasePojoResultMarshaller       (wr.ctx.viewName):        ToResponseMarshaller[PojoResult]    )(pj)
      case ls: ListResult     =>
        (toEntityQuereaseListResultMarshaller       (wr.ctx.viewName):        ToResponseMarshaller[ListResult]    )(ls)
      case nr: NumberResult   => (toEntityQuereaseNumberResultMarshaller:     ToResponseMarshaller[NumberResult]  )(nr)
      case cd: CodeResult     => (toEntityQuereaseCodeResultMarshaller:       ToResponseMarshaller[CodeResult]    )(cd)
      case id: IdResult       => (toEntityQuereaseIdResultMarshaller:         ToResponseMarshaller[IdResult]      )(id)
      case sr: StatusResult   => (toResponseQuereaseStatusResultMarshaller:   ToResponseMarshaller[StatusResult]  )(sr)
      case no: NoResult.type  => (toEntityQuereaseNoResultMarshaller:         ToResponseMarshaller[NoResult.type] )(no)
      case dr: QuereaseDelRes => (toEntityQuereaseDeleteResultMarshaller:     ToResponseMarshaller[QuereaseDelRes])(dr)
      case tq: TresqlResult   => sys.error("TresqlResult must be serialized before marshalling.")
      case rr: TresqlSingleRr => sys.error("TresqlSingleRowResult must be serialized before marshalling.")
      case it: IteratorResult => sys.error("IteratorResult must be serialized before marshalling.")
      case r: QuereaseResultWithCleanup =>
        sys.error(s"QuereaseResult marshaller for class ${r.getClass.getName} not implemented")
    }}

  implicit val toResponseQuereaseIteratorMarshaller: ToResponseMarshaller[qe.QuereaseIteratorResult[app.Dto]] = {
    def marsh(viewName: String)(implicit ec: ExecutionContext): ToResponseMarshaller[qe.QuereaseIteratorResult[app.Dto]] =
      Marshaller.combined { qir: qe.QuereaseIteratorResult[app.Dto] =>
        app.serializeResult(app.SerializationBufferSize, app.viewSerializationBufferMaxFileSize(viewName),
          DtoDataSerializer.source(() => qir), isCollection = true)
      } (GenericMarshallers.futureMarshaller(toEntityQuereaseSerializedResultMarshaller(viewName)))

    Marshaller { ec => res => marsh(res.view.name)(ec)(res) }
  }

  implicit def toResponseTresqlResultMarshaller(implicit res: Resources): ToEntityMarshaller[RowLike] =
    Marshaller { _ => tresqlResult =>
      val sr = app.serializeResult(app.SerializationBufferSize, app.SerializationBufferMaxFileSize,
        tresqlResult match {
          case result: Result[_] => TresqlResultSerializer.source(() => result)
          case row:    RowLike   => TresqlResultSerializer.rowSource(() => row)
        },
        tresqlResult.isInstanceOf[Result[_]],
        app.dbAccess.closeResources(res, _))
      GenericMarshallers
        .futureMarshaller(toEntityQuereaseSerializedResultMarshaller(null))(sr)
    }
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
