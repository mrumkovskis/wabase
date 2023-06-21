package org.wabase

import akka.stream.scaladsl._
import akka.http.scaladsl.coding.Coders.{Deflate, Gzip, NoCoding}
import akka.http.scaladsl.marshalling.{Marshaller, ToEntityMarshaller}
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.server._
import akka.http.scaladsl.server.Directives._
import StatusCodes._

import scala.language.postfixOps
import scala.util.matching.Regex
import spray.json._
import org.slf4j.LoggerFactory
import org.tresql.MissingBindVariableException

import scala.concurrent.duration._
import scala.concurrent.Future
import AppMetadata.Action
import AppMetadata.AugmentedAppViewDef
import AppServiceBase._
import Authentication.SessionUserExtractor
import DeferredControl._
import akka.http.scaladsl.model.MediaTypes.`application/json`

import java.net.URLEncoder
import java.util.Locale
import akka.http.scaladsl.server.util.Tuple
import akka.http.scaladsl.unmarshalling.FromRequestUnmarshaller
import akka.util.ByteString
import io.bullet.borer.{Json, Writer}
import org.mojoz.querease.{ValidationException, ValidationResult}

import java.lang.reflect.InvocationTargetException
import scala.util.{Failure, Success}


trait AppProvider[User] {
  type App <: AppBase[User]
  final val app: App = initApp
  /** Override this method in subclass to initialize {{{app}}}. */
  protected def initApp: App
}

trait AppServiceBase[User]
  extends AppProvider[User]
  with AppStateExtractor
  with JsonConverterProvider
  with DbAccessProvider
  with AppI18nService
  with Marshalling {
  this: QueryTimeoutExtractor with Execution =>

  import app.qe.metadataConventions
  import app.qio.MapJsonFormat

  private implicit lazy val fs: AppFileStreamer[User] = this match {
    case fsb: AppFileServiceBase[User@unchecked] => fsb.fileStreamer
    case _ => null
  }

  //custom directives
  def metadataPath = path("metadata" / Segment ~ Slash.?) & get
  def apiPath = path("api" ~ Slash.?) & get

  def crudPath = pathPrefix("data")
  // @deprecated("Use viewWithKeyPath. This method will be removed", "6.0.3")
  def viewWithIdPath = path(Segment / LongNumber)
  def viewWithKeyPath = path(Segment / Segments) | path(Segment ~ PathEnd) & provide(Nil: List[String])
  @deprecated("Use key without field name. This method will be removed", "6.0")
  def viewWithNamePath = path(Segment / Segment / Segment)
  def createPath = (path("create" / Segment) | pathPrefix("create:") & rawPathPrefix(Segment)) & get
  def viewWithoutIdPath = path(Segment ~ (PathEnd | Slash))
  // @deprecated("Use getByKeyPath. This method will be removed", "6.0.3")
  def getByIdPath = viewWithIdPath & get
  def getByKeyPath = viewWithKeyPath & get
  @deprecated("Use key without field name. This method will be removed", "6.0")
  def getByNamePath = viewWithNamePath & get
  // @deprecated("Use deleteByKeyPath. This method will be removed", "6.0.3")
  def deletePath = viewWithIdPath & delete
  def deleteByKeyPath = viewWithKeyPath & delete
  // @deprecated("Use updateByKeyPath. This method will be removed", "6.0.3")
  def updatePath = viewWithIdPath & put
  def updateByKeyPath = viewWithKeyPath & put
  def insertPath = viewWithoutIdPath & post
  def listOrGetPath = viewWithoutIdPath & get
  def countPath = (path("count" / Segment) | pathPrefix("count:") & rawPathPrefix(Segment)) & get

  def entityOrException[T](um: FromRequestUnmarshaller[T]): Directive1[T] =
    extractRequestContext.flatMap[Tuple1[T]] { ctx =>
      // import ctx.executionContext
      import ctx.materializer
      onComplete(um(ctx.request)) flatMap {
        case Success(value) => provide(value)
        case Failure(x)     => throw x
      }
    }

  private def extractStringId: Directive1[String] =
    extractMatchedPath flatMap { path =>
      provide(path.reverse.head.toString)
    }
  private def useActions(viewName: String, actionName: String) =
    !app.useLegacyFlow(viewName, actionName)

  // @deprecated("Use getByKeyAction. This method will be removed", "6.0.3")
  def getByIdAction(viewName: String, id: Long)(implicit user: User, state: ApplicationState, timeout: QueryTimeout) =
    parameterMultiMap { params =>
      if (useActions(viewName, Action.Get)) {
        extractRequest { implicit req =>
          extractStringId { idString =>
            complete(app.doWabaseAction(Action.Get, viewName, Seq(idString), filterPars(params)))
          }
        }
      } else {
        complete(app.get(viewName, id, filterPars(params)))
      }
    }

  def getByKeyAction(viewName: String, keyValues: Seq[String])(
    implicit user: User, state: ApplicationState, timeout: QueryTimeout) =
    parameterMultiMap { params =>
      if (useActions(viewName, Action.Get)) {
        extractRequest { implicit req =>
          complete(app.doWabaseAction(Action.Get, viewName, keyValues, filterPars(params)))
        }
      } else {
        val keyAsMap = app.prepareKey(viewName, keyValues, "get")
        complete(app.get(viewName, -1, filterPars(params) ++ keyAsMap))
      }
    }

  @deprecated("Use key without field name. This method will be removed", "6.0")
  def getByNameAction(viewName: String, name: String, value: String)(
    implicit user: User, state: ApplicationState, timeout: QueryTimeout) =
    parameterMultiMap { params =>
      if (useActions(viewName, Action.Get)) {
        extractRequest { implicit req =>
          complete(app.doWabaseAction(Action.Get, viewName, Seq(value), filterPars(params) + (name -> value)))
        }
      } else {
        complete(app.get(viewName, -1, filterPars(params) + (name -> value)))
      }
    }

  def createAction(viewName: String)(implicit user: User, state: ApplicationState, timeout: QueryTimeout) =
    parameterMultiMap { params =>
      if (useActions(viewName, Action.Create)) {
        extractRequest { implicit req =>
          complete(app.doWabaseAction(Action.Create, viewName, Nil, filterPars(params)))
        }
      } else {
        complete(app.create(viewName, filterPars(params)))
      }
    }

  // @deprecated("Use deleteByKeyAction. This method will be removed", "6.0.3")
  def deleteAction(viewName: String, id: Long)(implicit user: User, state: ApplicationState, timeout: QueryTimeout) =
    if (useActions(viewName, Action.Delete))
      extractStringId { idString =>
        deleteByKeyAction(viewName, Seq(idString))
      }
    else
      parameterMultiMap { params =>
        complete {
          app.delete(viewName, id, filterPars(params))
          StatusCodes.NoContent
        }
      }

  def deleteByKeyAction(viewName: String, keyValues: Seq[Any])(
    implicit user: User, state: ApplicationState, timeout: QueryTimeout): Route =
    parameterMultiMap { params =>
      extractRequest { implicit req =>
        complete {
          app.doWabaseAction(Action.Delete, viewName, keyValues, filterPars(params))
        }
      }
    }

  // @deprecated("Use updateByKeyAction. This method will be removed", "6.0.3")
  def updateAction(viewName: String, id: Long)(
    implicit user: User, state: ApplicationState, timeout: QueryTimeout): Route =
    if (useActions(viewName, Action.Update))
      extractStringId { idString =>
        updateByKeyAction(viewName, Seq(idString))
      }
    else
      extractUri { requestUri =>
        parameterMultiMap { params =>
          entityOrException(as[JsValue]) { data =>
            app.save(viewName, data.asInstanceOf[JsObject], filterPars(params))
            redirect(Uri(path = requestUri.path), StatusCodes.SeeOther)
          }
        }
      }

  def updateByKeyAction(viewName: String, keyValues: Seq[Any])(
    implicit user: User, state: ApplicationState, timeout: QueryTimeout): Route =
      parameterMultiMap { params =>
        app.checkApi(viewName, Action.Update, user)
        implicit val um = toMapUnmarshallerForView(viewName)
        entityOrException(as[Map[String, Any]]) { entityAsMap =>
          extractRequest { implicit req =>
            complete {
              app.doWabaseAction(Action.Update, viewName, keyValues, filterPars(params), entityAsMap,
                doApiCheck = false /* api checked above */)
            }
          }
        }
      }

  def listOrGetAction(viewName: String)(implicit user: User, state: ApplicationState, timeout: QueryTimeout) =
    parameterMultiMap { params =>
      val impliedIdForGetOpt = app.impliedIdForGetOverList(viewName)
      if (impliedIdForGetOpt.isDefined)
        if (useActions(viewName, Action.Get)) {
          extractRequest { implicit req =>
            complete(app.doWabaseAction(Action.Get, viewName, Nil, filterPars(params)))
          }
        } else {
          complete(app.get(viewName, impliedIdForGetOpt.get, filterPars(params)))
        }
      else
        listAction(viewName, params)
    }

  protected def listAction(viewName: String, params: Map[String, List[String]])(
    implicit user: User, state: ApplicationState, timeout: QueryTimeout) =
    if (useActions(viewName, Action.List)) {
      extractRequest { implicit req =>
        complete {
          app.doWabaseAction(
            Action.List,
            viewName,
            Nil,
            filterPars(params) ++
              params.filter { case (k, v) =>
                k == "offset" ||
                  k == "limit" ||
                  k == "sort"
              }.map { case (k, v) => (k, v.headOption.orNull) },
          )
        }
      }
    }
    else complete {
      app.list(
        viewName,
        filterPars(params),
        params.get("offset").flatMap(_.headOption).map(_.toInt) getOrElse 0,
        params.get("limit").flatMap(_.headOption).map(_.toInt) getOrElse 0,
        params.get("sort").flatMap(_.headOption).orNull)
    }

  def insertAction(viewName: String)(implicit user: User, state: ApplicationState, timeout: QueryTimeout) =
    extractUri { requestUri =>
      parameterMultiMap { params =>
        if (useActions(viewName, Action.Insert)) {
          app.checkApi(viewName, Action.Insert, user)
          implicit val um = toMapUnmarshallerForView(viewName)
          entityOrException(as[Map[String, Any]]) { entityAsMap =>
            extractRequest { implicit req =>
              complete {
                app.doWabaseAction(Action.Insert, viewName, Nil, filterPars(params), entityAsMap,
                  doApiCheck = false /* api checked above */)
              }
            }
          }
        } else {
          entityOrException(as[JsValue]) { data =>
            val id = app.save(viewName, data.asInstanceOf[JsObject], filterPars(params))
            redirect(Uri(path = requestUri.path / id.toString), StatusCodes.SeeOther)
          }
        }
      }
    }

  def countAction(viewName: String)(implicit user: User, state: ApplicationState, timeout: QueryTimeout) =
    parameterMultiMap { params =>
      if (useActions(viewName, Action.Count)) {
        extractRequest { implicit req =>
          complete(app.doWabaseAction(Action.Count, viewName, Nil, filterPars(params)))
        }
      } else {
        complete(app.count(viewName, filterPars(params)).toString)
      }
    }

  def filterPars(params: Map[String, List[String]]) =
    params.get("filter")
      .flatMap(_.headOption)
      .map(_.parseJson.convertTo[Map[String, Any]])
      .getOrElse(decodeParams(params))

  // OK to use deprecated getByIdPath, deletePath, updatePath here
  @annotation.nowarn("cat=deprecation")
  def crudActionOnKeyInPath(implicit user: User) = applicationState { implicit state =>
    extractTimeout { implicit timeout =>
      getByIdPath     { getByIdAction     } ~
      countPath       { countAction       } ~
      createPath      { createAction      } ~
      deletePath      { deleteAction      } ~
      deleteByKeyPath { deleteByKeyAction } ~
      updatePath      { updateAction      } ~
      updateByKeyPath { updateByKeyAction } ~
      listOrGetPath   { listOrGetAction   } ~
      getByKeyPath    { getByKeyAction    } ~
      insertPath      { insertAction      }
    }
  }

  def crudAction(implicit user: User) =
    pathPrefixTest(Segment ~ PathEnd) { _ =>
      mapRequestContext(keyFromQueryToPath) {
        crudActionOnKeyInPath
      }
    } ~ pathPrefixTest(!(Segment ~ PathEnd)) {
      crudActionOnKeyInPath
    }

  /** Enables alternative URI where row key is in special query string */
  def keyFromQueryToPath(context: RequestContext): RequestContext = {
    def decode(s: String) = java.net.URLDecoder.decode(s, "UTF-8")
    context.request.uri.rawQueryString match {
      case Some(rawQ) if rawQ startsWith "/" =>
        val (p, q) = rawQ.indexOf('?') match {
          case -1 => (rawQ, null)
          case i  => (rawQ.substring(0, i), rawQ.substring(i + 1))
        }
        @annotation.tailrec
        def toPath(path: Uri.Path, p: String): Uri.Path = p.indexOf('/', 1) match {
          case -1 => path / decode(p.substring(1))
          case i  => toPath(path / decode(p.substring(1, i)), p.substring(i))
        }
        val unmatchedPath = toPath(context.unmatchedPath, p)
        val uriWithPath   = context.request.uri.withPath(toPath(context.request.uri.path, p))
        val uri =
          if (q == null)
               uriWithPath.withQuery(Uri.Query.Empty)
          else uriWithPath.withRawQueryString(q)
        context
          .withUnmatchedPath(unmatchedPath)
          .withRequest(context.request.withUri(uri))
      case _ => context
    }
  }

  def apiAction(implicit user: User) = complete(app.api)
  def metadataAction(viewName: String)(implicit user: User, state: ApplicationState) =
    respondWithHeader(ETag(EntityTag(app.metadataVersionString))) {
      conditional(EntityTag(app.metadataVersionString), DateTime.now) {
        val obj = if (viewName == "*") app.apiMetadata else app.metadata(viewName)
        complete(obj)
      }
  }

  val DefaultResourceExtensions = "js,css,html,png,gif,jpg,jpeg,svg,woff,ttf,woff2".split(",").toSet
  val DefaultResourcePathBase = "app"
  def staticResources(extensions: Set[String] = DefaultResourceExtensions, basePath: String = DefaultResourcePathBase): Route =
    pathSuffixTest(new Regex(extensions.map("\\." + _).mkString(".*(", "|", ")$"))) { p =>
      path(Remaining) { resource =>
        (encodeResponseWith(NoCoding, Gzip, Deflate) & respondWithHeader(ETag(EntityTag(app.metadataVersionString)))) {
          getFromResource(basePath + "/" + resource)
        }
      }
    }
  def decodeParams(params: Map[String, List[String]]): Map[String, Any] = params map { t =>
    t._1 -> (t._2.map(decodeParam(t._1, _)) match {
      case List(x) => x
      case x @ List(_, _*) => x
      case x => throw new IllegalStateException("unexpected: " + x)
    })
  }
  def decodeMultiParams(params: Map[String, List[String]]) = params map { t => t._1 -> t._2.map(decodeParam(t._1, _)) }
  val namesForInts = Set("limit", "offset")
  def escapeReflectedXss(msg: String) =
    msg.replace("<", "[<]")
  def decodeParam(key: String, value: String) = {
    def throwBadType(type_ : String, cause: Exception = null) =
      throw new BusinessException(escapeReflectedXss(
        s"Failed to decode as $type_: parameter: '$key', value: '$value'" +
          (if (cause == null) "" else " - caused by " + cause.toString)))
    def handleType[T](goodPath: String => T, typeStr:String)= {
      try value match {
        case "" | "null" | null => null
        case d => goodPath(d)
      } catch {
        case ex: Exception => throwBadType(typeStr, ex)
      }
    }
    if (metadataConventions.isBooleanName(key)) {
      handleType({
        case "true" => TRUE
        case "false" => FALSE
      }, "boolean")
    } else if (metadataConventions.isDateName(key)) {
          handleType(d => new java.sql.Date(Format.parseDate(d.replaceAll("\"", "")).getTime),"date")
    } else if (metadataConventions.isDateTimeName(key)) {
          handleType(d => new java.sql.Timestamp(Format.parseDateTime(d.replaceAll("\"", "")).getTime), "dateTime (not supported yet)")
    } else if (namesForInts.contains(key) ||
               metadataConventions.isIntegerName(key) ||
               metadataConventions.isIdName(key) ||
               metadataConventions.isIdRefName(key)) {
          handleType(l => java.lang.Long.valueOf(l), "long")
    } else if (metadataConventions.isDecimalName(key)) {
        handleType(d => BigDecimal(d), "bigDecimal")
    } else value
  }
  override protected def initJsonConverter = app.qio
  override def dbAccess = app.dbAccess

  protected def fileStreamerConfigs: Seq[AppFileStreamerConfig] = {
    Option(this)
      .collect { case fs: AppFileServiceBase[_] => fs.fileStreamer }
      .toList ++
      Option(this)
        .flatMap {
          case dc: DeferredControl => dc.fileStreamerConfig
          case _ => None
        }
        .toList
  }
}

trait AppFileServiceBase[User] {
    this: AppProvider[User] with JsonConverterProvider with BasicJsonMarshalling
          { type App <: AppBase[User] with Audit[User] } =>
  val fileStreamer: AppFileStreamer[User] = initFileStreamer
  /** Override this method in subclass. Method usage instead of direct
  {{{val fileStreamer: AppFileStreamer}}} initialization ensures that this.fileStreamer and subclass fileStreamer
  have the same instance in the case fileStreamer is overrided in subclass */
  protected def initFileStreamer: AppFileStreamer[User]
  def uploadPath: Directive1[Option[String]] =
    path("upload") & provide(None) |
    path("upload" / Segment).flatMap { filename => provide(Some(filename))}
  def uploadMultiplePath = path("upload-multiple")
  def downloadPath = path("download" / LongNumber / Segment) & get
  def uploadSizeLimit =  config.getBytes("app.upload.size-limit").toLong

  //make visible implicit querease for fileInfo methods
  private implicit val qe: AppQuerease = DefaultAppQuerease
  import AppFileStreamer._
  def validateFileName(fileName: String) = {}

  def extractFileDirective(filenameOpt: Option[String])(implicit user: User, state: ApplicationState): Directive[(Source[ByteString, Any], String, String)] =
    (withSizeLimit(uploadSizeLimit) & post & extractRequestContext).flatMap { ctx =>
      def multipartFormUpload = {
        entity(as[Multipart.FormData]).flatMap { _ =>
          fileUpload("file").flatMap {
            case (fileInfo, bytes) =>
              validateFileName(fileInfo.fileName)
              provide(bytes) & provide(fileInfo.fileName) & provide(fileInfo.contentType.toString)
          }
        }
      }
      def simpleUpload(fileName: String) = {
        val contentType = ctx.request.entity.contentType
        validateFileName(fileName)
        provide (ctx.request.entity.dataBytes) & provide(fileName) & provide(contentType.toString)
      }
      filenameOpt match {
        case None =>
          multipartFormUpload | simpleUpload("file")
        case Some(fileName) =>
          simpleUpload(fileName)
      }
    }

  def uploadFileDirective(bytes: Source[ByteString, Any],
                          fileName: String,
                          contentType: String
                         )(implicit
                          user: User,
                          state: ApplicationState
                         ): Directive1[Future[FileInfo]] =
    extractRequestContext.map { ctx =>
      import ctx._
      bytes.runWith(fileStreamer.fileSink(fileName, contentType)).andThen {
        case scala.util.Success(fileInfo) => app.auditSave(fileInfo.id, fileStreamer.file_info_table, fileInfo.toMap, null)
        case scala.util.Failure(error) => app.auditSave(null, fileStreamer.file_info_table,
          Map("filename" -> fileName, "content_type" -> contentType.toString), error.getMessage)
      }
    }


  implicit class DirectiveChain1[A](directive: Directive[(A)]) {
    def andThen[T](fun: A => Directive[T])(implicit arg0: Tuple[T]): Directive[T] =
      directive.tflatMap { case (a) => fun(a) }
  }

  implicit class DirectiveChain2[A, B](directive: Directive[(A, B)]) {
    def andThen[T](fun: (A, B) => Directive[T])(implicit arg0: Tuple[T]): Directive[T] =
      directive.tflatMap { case (a, b) => fun(a, b) }
  }

  implicit class DirectiveChain3[A, B, C](directive: Directive[(A, B, C)]) {
    def andThen[T](fun: (A, B, C) => Directive[T])(implicit arg0: Tuple[T]): Directive[T] =
      directive.tflatMap { case (a, b, c) => fun(a, b, c) }
  }

  def uploadAction(filenameOpt: Option[String])(implicit
          user: User,
          state: ApplicationState
         ): Route = {
    val ufd = extractFileDirective(filenameOpt).andThen(uploadFileDirective _).flatMap(onSuccess(_))
    ufd(fi => complete(fi.toMap))
  }

  def uploadMultipleAction(implicit
      user: User,
      state: ApplicationState,
  ): Route = withSizeLimit(uploadSizeLimit) {
    (post & uploadMultiple) { partsInfoFuture =>
      extractRequestContext { ctx =>
        import ctx._
        ctx => complete(partsInfoFuture.map(_.map(_.toMap).toList))
      }
    }
  }

  def uploadMultiple(implicit
        user: User,
        state: ApplicationState
    ): Directive1[Future[Seq[PartInfo]]] = {
    (entity(as[Multipart.FormData]) & extractRequestContext).tflatMap { case (formdata, ctx) =>
      provide {
        import ctx._
        formdata.parts.mapAsync(1) {
          case filePart if filePart.filename.isDefined =>
            val name = filePart.name
            val filename = filePart.filename.getOrElse("file")
            val contentTypeString =
              Option(filePart.entity.contentType.toString)
                .getOrElse("application/octet-stream")
            val bytes = filePart.entity.dataBytes
            bytes.runWith(fileStreamer.fileSink(filename, contentTypeString))
              .map { fileInfo =>
                PartInfo(
                  name = name,
                  value = null,
                  file_info = fileInfo,
                )
              }.andThen { // audit file save
                case scala.util.Success(partInfo) =>
                  val fileInfo = partInfo.file_info
                  app.auditSave(fileInfo.id, fileStreamer.file_info_table, fileInfo.toMap, null)
                case scala.util.Failure(error) => app.auditSave(null, fileStreamer.file_info_table,
                  Map("filename" -> filename, "content_type" -> contentTypeString), error.getMessage)
              }
          case dataPart =>
            dataPart.toStrict(1.second).map { strict =>
              PartInfo(
                name = dataPart.name,
                value = strict.entity.data.utf8String,
                file_info = null,
              )
            }
        }.runFold(Seq.empty[PartInfo])(_ :+ _)
      }
    }
  }

  def downloadAction(fileInfoHelperOpt: Option[FileInfoHelper])(implicit user: User, state: ApplicationState): Route = {
    fileInfoHelperOpt match {
      case Some(fi) =>
        complete(HttpResponse(
          StatusCodes.OK,
          contentDisposition(fi.filename, ContentDispositionTypes.attachment),
          HttpEntity.Default(
            // This will always be MediaType.Binary, if 2nd param is true
            // application/octet-stream as a fallback
            MediaType.custom(
              Option(fi.content_type).filter(_ != "null").filter(_ != "") getOrElse "application/octet-stream",
              true,
            ).asInstanceOf[MediaType.Binary],
            fi.size,
            fi.source
          )
        ))
      case None => complete(StatusCodes.NotFound)
    }
  }

  def downloadAction(id: Long, sha256: String)(implicit user: User, state: ApplicationState): Route =
    downloadAction(fileStreamer.getFileInfo(id, sha256))
}

object AppServiceBase {

  trait AppStateExtractor { this: AppServiceBase[_] with QueryTimeoutExtractor with Execution =>
    val ApplicationStateCookiePrefix = "current_"
    def applicationState = extract(r => extractState(r.request, ApplicationStateCookiePrefix))
    protected def extractState(req: HttpRequest, prefix: String) = {
      val state = req.headers.flatMap {
        case c: Cookie => c.cookies.filter(_.name.startsWith(prefix))
        case _ => Nil
      } map (c => c.name -> decodeParam(c.name, c.value)) toMap
      val langKey = ApplicationStateCookiePrefix + ApplicationLanguageCookiePostfix
      if (state.contains(langKey))
        ApplicationState(state, new Locale(String.valueOf(state(langKey))))
      else
        currentLangFromHeader(req)
          .map(l => ApplicationState(state + (langKey -> l), new Locale(l)))
          .getOrElse(ApplicationState(state))
    }
  }

  trait AppVersion {
    def appVersion: String
  }

  trait QueryTimeoutExtractor {
    def maxQueryTimeout: QueryTimeout = QueryTimeout(5)
    lazy val queryTimeout: QueryTimeout = Option(DefaultQueryTimeout)
      .orElse(Some(maxQueryTimeout))
      .filter(_.timeoutSeconds <= maxQueryTimeout.timeoutSeconds)
      .getOrElse {
        LoggerFactory.getLogger("JdbcTimeoutLogger")
          .error(s"Illegal configuration for jdbc.query-timeout setting = $DefaultQueryTimeout. " +
            s"Must be less than or equal to $maxQueryTimeout.")
        maxQueryTimeout
      }

    def extractTimeout: Directive1[QueryTimeout]
  }

  /** Always returns queryTimeout */
  trait ConstantQueryTimeout extends QueryTimeoutExtractor {
    override def extractTimeout = extract(_ => queryTimeout)
  }

  trait AppExceptionHandler {
    val appExceptionHandler: ExceptionHandler
  }

  object AppExceptionHandler{
    def entityStreamSizeExceptionHandler(marshalling: BasicJsonMarshalling) = ExceptionHandler {
      case e: EntityStreamSizeException =>
        import marshalling._
        val response = Map[String, Any]("actualSize"-> e.actualSize.orNull, "limit" -> e.limit)
        complete(StatusCodes.PayloadTooLarge -> response)
    }

    def businessExceptionHandler(logger: com.typesafe.scalalogging.Logger) = ExceptionHandler {
      case e: BusinessException =>
        logger.trace(e.getMessage, e)
        complete(HttpResponse(BadRequest, entity = e.getMessage))
      case e: InvocationTargetException if e.getCause != null && e.getCause.isInstanceOf[BusinessException] =>
        logger.trace(e.getMessage, e)
        val msg = Option(e.getCause.getMessage).getOrElse("")
        complete(HttpResponse(BadRequest, entity = msg))
    }

    def unprocessableEntityExceptionHandler(logger: com.typesafe.scalalogging.Logger) = ExceptionHandler {
      case e: UnprocessableEntityException =>
        logger.trace(e.getMessage, e)
        complete(HttpResponse(UnprocessableEntity, entity = e.getMessage))
    }

    def bindVariableExceptionHandler(logger: com.typesafe.scalalogging.Logger,
        bindVariableExceptionResponseMessage: MissingBindVariableException => String = _.getMessage) = ExceptionHandler {
      case e: MissingBindVariableException =>
        logger.debug(e.getMessage, e)
        complete(HttpResponse(BadRequest, entity = bindVariableExceptionResponseMessage(e)))
    }

    def quereaseEnvExceptionHandler(logger: com.typesafe.scalalogging.Logger) = ExceptionHandler {
      case e: QuereaseEnvException =>
        logger.debug(e.getMessage, e)
        complete(HttpResponse(BadRequest, entity = e.getMessage))
    }

    def viewNotFoundExceptionHandler = ExceptionHandler {
      case e: org.mojoz.querease.ViewNotFoundException => complete(HttpResponse(NotFound, entity = e.getMessage))
    }

    def rowNotFoundExceptionHandler = ExceptionHandler ({
      case e: org.mojoz.querease.NotFoundException =>
        complete(NotFound)
    })

    def validationExceptionHandler(logger: com.typesafe.scalalogging.Logger) = ExceptionHandler {
      case e: ValidationException =>
        logger.trace(e.getMessage, e)
        complete(HttpResponse(BadRequest, entity = e.getMessage))
    }

    def validationExceptionPathsHandler(logger: com.typesafe.scalalogging.Logger,
                                        jsonConverter: JsonConverter[_]) = ExceptionHandler {
      case e: ValidationException =>
        logger.trace(e.getMessage, e)
        import spray.json.DefaultJsonProtocol.{ jsonFormat2, listFormat, StringJsonFormat }
        import jsonConverter._
        implicit val f02: RootJsonFormat[ValidationResult] = jsonFormat2(ValidationResult)
        complete(HttpResponse(BadRequest, entity = e.details.toJson.compactPrint))
    }

    /** Handles and logs PostgreSQL timeout exceptions */
    trait PostgresTimeoutExceptionHandler[User] extends AppExceptionHandler {
      this: AppStateExtractor
       with AppServiceBase[_]
       with QueryTimeoutExtractor
       with Execution
       with SessionUserExtractor[User]
       with ServerStatistics
       with DeferredCheck
       with AppI18nService =>
      override val appExceptionHandler = PostgresTimeoutExceptionHandler(this)
    }

    object PostgresTimeoutExceptionHandler {
      val timeoutLogger = LoggerFactory.getLogger("JdbcTimeoutLogger")
      val TimeoutSignature = "ERROR: canceling statement due to user request"
      val TimeoutFriendlyMessage = "Request canceled due to too long processing time"
      def apply[User](
        appService: AppStateExtractor with SessionUserExtractor[User]
          with ServerStatistics with DeferredCheck with AppI18nService) = ExceptionHandler {
       case e: org.postgresql.util.PSQLException if e.getMessage.startsWith(TimeoutSignature) =>
        import appService._
        registerTimeout
        (extractUserFromSession & extractRequest & applicationState) { (userOpt, req, appState) =>
          val aState = appState.state.map{ case (k,v) => s"$k = $v" }.mkString("{", ", ", "}")
          val userString = userOpt.map(_.toString).orNull
          val msg = s"JDBC timeout, statement cancelled - ${req.method} ${req.uri}, state - $aState, user - $userString"
          isDeferred
          .tmap { _ =>
            timeoutLogger.error("Deferred " + msg)
          }.recover { _ =>
            timeoutLogger.error(msg)
            pass
          }.apply { //somehow apply method must be called explicitly ???
            complete(HttpResponse(InternalServerError,
              entity = i18n.translate(TimeoutFriendlyMessage)(getApplicationLocale(appState))))
          }
        }
      }
    }

    object TresqExceptionHandler {
      def apply[User](
        appService: AppStateExtractor with SessionUserExtractor[User]
          with ServerStatistics with DeferredCheck with AppI18nService) = ExceptionHandler {
        case e: org.tresql.TresqlException if e.getCause.isInstanceOf[org.postgresql.util.PSQLException] &&
          e.getCause.getMessage == PostgresTimeoutExceptionHandler.TimeoutSignature =>
          PostgresTimeoutExceptionHandler(appService)(e.getCause)
      }
    }

    /** Handles [[org.wabase.BusinessException]]s and [[org.tresql.MissingBindVariableException]]s and
      * [[org.mojoz.querease.ViewNotFoundException]]*/
    trait SimpleExceptionHandler extends AppExceptionHandler { this: Loggable =>
      def bindVariableExceptionResponseMessage(e: MissingBindVariableException): String = e.getMessage
      override val appExceptionHandler =
        unprocessableEntityExceptionHandler(this.logger)
          .withFallback(businessExceptionHandler(this.logger))
          .withFallback(bindVariableExceptionHandler(this.logger, this.bindVariableExceptionResponseMessage))
          .withFallback(quereaseEnvExceptionHandler(this.logger))
          .withFallback(viewNotFoundExceptionHandler)
          .withFallback(rowNotFoundExceptionHandler)
    }

    trait DefaultAppExceptionHandler[User] extends SimpleExceptionHandler with PostgresTimeoutExceptionHandler[User] {
      this: AppStateExtractor
        with AppServiceBase[_]
        with QueryTimeoutExtractor
        with Execution
        with SessionUserExtractor[User]
        with ServerStatistics
        with Loggable
        with DeferredCheck
        with BasicJsonMarshalling
        with AppI18nService =>
      override val appExceptionHandler =
        unprocessableEntityExceptionHandler(this.logger)
          .withFallback(businessExceptionHandler(this.logger))
          .withFallback(validationExceptionHandler(this.logger))
          .withFallback(entityStreamSizeExceptionHandler(this))
          .withFallback(bindVariableExceptionHandler(this.logger, this.bindVariableExceptionResponseMessage))
          .withFallback(quereaseEnvExceptionHandler(this.logger))
          .withFallback(PostgresTimeoutExceptionHandler(this))
          .withFallback(TresqExceptionHandler(this))
          .withFallback(viewNotFoundExceptionHandler)
          .withFallback(rowNotFoundExceptionHandler)
    }
  }

  trait AppI18nService { this: AppServiceBase[_] with QueryTimeoutExtractor with Execution =>
    val ApplicationLanguageCookiePostfix = "lang"

    val i18n: I18n = initI18n
    protected def initI18n: I18n = app

    def i18nPath = pathPrefix("i18n") & get
    def i18nLanguagePath = path("lang" / Segment)
    def i18nResourcePath = i18nPath & path(Segment ~ Slash.?)
    def i18nTranslatePath = i18nPath & path(Segment / Segment / RemainingPath ~ Slash.?)

    protected def langCookieTransformer(cookie: HttpCookie): HttpCookie = cookie

    def setLanguage: Route = (i18nPath & i18nLanguagePath) { lang =>
      setCookie(langCookieTransformer(
        HttpCookie(ApplicationStateCookiePrefix + ApplicationLanguageCookiePostfix,
          value = lang,
          path = Some("/")
        ).withSameSite(SameSite.Lax))
      ) { complete("Ok") }
    }

    def i18nResources: Route = (i18nPath & applicationLocale) { implicit locale =>
      complete(i18n.i18nResources)
    }

    def i18nResourcesFromBundle: Route = (i18nPath & i18nResourcePath) { resources =>
      applicationLocale { implicit locale =>
        complete(i18n.i18nResourcesFromBundle(resources))
      }
    }

    def i18nTranslate: Route = (i18nPath & i18nTranslatePath) { (name, key, params) =>
      applicationLocale { implicit locale =>
        import akka.http.scaladsl.model.Uri._
        def paramsList(path: Path): List[String] = path match {
          case Path.Empty => Nil
          case _: Path.Slash => paramsList(path.tail)
          case Path.Segment(h, t) => h :: paramsList(t)
        }
        complete(i18n.translateFromBundle(name, key, paramsList(params): _*))
      }
    }

    def currentLangFromHeader(request: HttpRequest) = {
      LanguageNegotiator(request.headers)
        .acceptedLanguageRanges
        .headOption
        .map(l => l.primaryTag +: l.subTags)
        .map(_.mkString("-"))
    }

    def applicationLocale = applicationState.map(getApplicationLocale)

    def getApplicationLocale(state: ApplicationState): Locale =
      state.state.get(ApplicationStateCookiePrefix + ApplicationLanguageCookiePostfix)
        .map(l => new Locale(String.valueOf(l)))
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
        })
      )
      HttpEntity.Chunked.fromData(`application/json`, source)
    }
  }
}
