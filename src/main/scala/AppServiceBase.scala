package org.wabase

import org.apache.pekko.stream.scaladsl._
import org.apache.pekko.http.scaladsl.coding.Coders.{Deflate, Gzip, NoCoding}
import org.apache.pekko.http.scaladsl.marshalling.ToEntityMarshaller
import org.apache.pekko.http.scaladsl.model._
import org.apache.pekko.http.scaladsl.model.headers._
import org.apache.pekko.http.scaladsl.server._
import org.apache.pekko.http.scaladsl.server.Directives._
import StatusCodes._

import scala.language.postfixOps
import scala.util.matching.Regex
import org.mojoz.querease.ValueConverter.{ClassOfJavaSqlDate, ClassOfJavaSqlTimestamp}
import org.slf4j.LoggerFactory
import org.tresql.MissingBindVariableException

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import AppMetadata.Action
import AppMetadata.AugmentedAppViewDef
import AppServiceBase._
import Authentication.SessionUserExtractor
import DeferredControl._
import com.typesafe.scalalogging.Logger
import org.apache.pekko.actor.ActorSystem

import java.util.Locale
import org.apache.pekko.http.scaladsl.server.util.Tuple
import org.apache.pekko.http.scaladsl.unmarshalling.FromRequestUnmarshaller
import org.apache.pekko.util.ByteString
import org.mojoz.querease.{ValidationException, ValidationResult}
import org.wabase.ds.ConnectionPools.DefaultQueryTimeout
import org.wabase.ds.QueryTimeout
import org.wabase.handlers.CSRFException

import java.lang.reflect.InvocationTargetException
import scala.util.{Failure, Success}
import scala.jdk.CollectionConverters._


trait AppProvider[User] {
  type App <: AppBase[User]
  final val app: App = initApp
  /** Override this method in subclass to initialize {{{app}}}. */
  protected def initApp: App
}

trait AppServiceBase[User]
  extends AppProvider[User]
  with AppStateExtractor
  with DbAccessProvider
  with AppI18nService
  with Marshalling {
  this: QueryTimeoutExtractor with Execution =>

  import app.qe.metadataConventions

  private implicit lazy val fs: AppFileStreamer[User] = this match {
    case fsb: AppFileServiceBase[User@unchecked] => fsb.fileStreamer
    case _ => null
  }

  //custom directives
  def metadataPath = path("metadata" / Segment ~ Slash.?) & get
  def apiPath = path("api" ~ Slash.?) & get

  def crudPath = pathPrefix("data")
  def viewWithIdPath = path(Segment / LongNumber)
  def viewWithKeyPath = path(Segment / Segments) | path(Segment ~ PathEnd) & provide(Nil: List[String])
  def createPath = (path("create" / Segment) | path("""(.+):create""".r)) & get
  def viewWithoutIdPath = path(Segment ~ (PathEnd | Slash))
  def getByIdPath = viewWithIdPath & get
  def getByKeyPath = viewWithKeyPath & get
  def deleteByIdPath  = viewWithIdPath    & delete
  def deleteByKeyPath = viewWithKeyPath   & delete
  def putByIdPath     = viewWithIdPath    & put
  def putByKeyPath    = viewWithKeyPath   & put
  def postByKeyPath   = viewWithKeyPath   & post
  def listOrGetPath   = viewWithoutIdPath & get
  def countPath = (path("count" / Segment) | path("""(.+):count""".r)) & get

  def entityOrException[T](um: FromRequestUnmarshaller[T]): Directive1[T] =
    extractRequestContext.flatMap[Tuple1[T]] { ctx =>
      import ctx.materializer
      onComplete(um(ctx.request)) flatMap {
        case Success(value) => provide(value)
        case Failure(x)     => throw x
      }
    }

  def entityAsMapOrException(viewName: String): Directive1[Map[String, Any]] =
    app.qe.viewDef(viewName).decoder match {
      case AppMetadata.DefaultDecoder =>
        def defaultContent = extractRequestContext.flatMap { ctx =>
          import ctx.materializer
          val um = toMapUnmarshallerForView(viewName)
          onComplete(um(ctx.request.entity)) flatMap {
            case Success(value) => provide(value)
            case Failure(x) => throw x
          }
        }
        def mappedContent = extractRequestContext.flatMap { ctx =>
          import ctx.materializer
          val um = toMapUnmarshaller
          onComplete(um(ctx.request.entity)) flatMap {
            case Success(m) => provide(app.qe.toCompatibleMap(m, app.qe.viewDef(viewName)))
            case Failure(x) => throw x
          }
        }
        extractRequestEntity.map(_.contentType).flatMap {
          case ContentTypes.`application/json` => defaultContent
          case ContentTypes.`application/x-www-form-urlencoded` =>
            mappedContent
          case multipartFormData if WabaseUnmarshallers.isMultipartFormData(multipartFormData.mediaType) =>
            mappedContent
          case _ => defaultContent
        }
      case AppMetadata.CustomDecoder(o, f) =>
        extractRequestContext.flatMap { ctx =>
          val value: Future[Map[String, Any]] = invokeFunction(o, f,
            Seq[(Class[_], () => Any)](
              (classOf[HttpRequest], () => ctx.request),
              (classOf[ActorSystem], () => actorSystem),
              (classOf[ExecutionContext], () => executionContext)
            )
          ) match {
            case f: Future[_] => f.mapTo[Map[String, Any]]
            case m: Map[String, Any]@unchecked => Future.successful(m)
            case x => throw new IllegalArgumentException(
              s"Custom decoder must return Map[String, Any], instead got: ${Option(x).map(_.getClass.getName).orNull}")
          }
          onComplete(value) flatMap {
            case Success(v) => provide(v)
            case Failure(x) => throw x
          }
        }
      case AppMetadata.NoneDecoder => provide(Map())
    }

  private def extractStringId: Directive1[String] =
    extractMatchedPath flatMap { path =>
      provide(path.reverse.head.toString)
    }
  private def useActions(viewName: String, actionName: String) =
    !app.useLegacyFlow(viewName, actionName)

  def getByIdAction(viewName: String, id: Long)(implicit user: User, state: ApplicationState, timeout: QueryTimeout) =
    parameterMultiMap { params =>
      if (useActions(viewName, Action.Get)) {
        extractRequest { implicit httpReq =>
          extractStringId { idString =>
            implicit val routeLogger: Logger = WabaseService.routeLogger(httpReq)
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
        extractRequest { implicit httpReq =>
          implicit val routeLogger: Logger = WabaseService.routeLogger(httpReq)
          complete(app.doWabaseAction(Action.Get, viewName, keyValues, filterPars(params)))
        }
      } else {
        val keyAsMap = app.prepareKey(viewName, keyValues, "get")
        complete(app.get(viewName, -1, filterPars(params) ++ keyAsMap))
      }
    }

  def createAction(viewName: String)(implicit user: User, state: ApplicationState, timeout: QueryTimeout) =
    parameterMultiMap { params =>
      if (useActions(viewName, Action.Create)) {
        extractRequest { implicit httpReq =>
          implicit val routeLogger: Logger = WabaseService.routeLogger(httpReq)
          complete(app.doWabaseAction(Action.Create, viewName, Nil, filterPars(params)))
        }
      } else {
        complete(app.create(viewName, filterPars(params)))
      }
    }

  def deleteByIdAction(viewName: String, id: Long)(implicit user: User, state: ApplicationState, timeout: QueryTimeout) =
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
      extractRequest { implicit httpReq =>
        complete {
          implicit val routeLogger: Logger = WabaseService.routeLogger(httpReq)
          app.doWabaseAction(Action.Delete, viewName, keyValues, filterPars(params))
        }
      }
    }

  val ActionForHttpPost = config.getString("app.action-for-http.post") // maybe "insert" for legacy app
  val ActionForHttpPut  = config.getString("app.action-for-http.put")  // maybe "update" for legacy app
  def putByIdAction(viewName: String, id: Long)(
    implicit user: User, state: ApplicationState, timeout: QueryTimeout): Route =
    if (useActions(viewName, ActionForHttpPut))
      extractStringId { idString =>
        putByKeyAction(viewName, Seq(idString))
      }
    else
      extractUri { requestUri =>
        parameterMultiMap { params =>
          entityAsMapOrException(viewName) { data =>
            app.save(viewName, data, filterPars(params))
            redirect(Uri(path = requestUri.path), StatusCodes.SeeOther)
          }
        }
      }

  def putByKeyAction(viewName: String, keyValues: Seq[Any])(
    implicit user: User, state: ApplicationState, timeout: QueryTimeout): Route =
      extractUri { requestUri =>
        parameterMultiMap { params =>
          extractRequest { implicit httpReq =>
            implicit val routeLogger: Logger = WabaseService.routeLogger(httpReq)
            onSuccess(app.checkApi(viewName, requestUri.path, ActionForHttpPut, user, keyValues)(
              AuthContext(actorSystem, httpReq, timeout, routeLogger))) { actionName =>
              entityAsMapOrException(viewName) { entityAsMap =>
                complete {
                  app.doWabaseAction(actionName, viewName, keyValues, filterPars(params), entityAsMap,
                    doApiCheck = false /* api checked above */)
                }
              }
            }
          }
        }
      }

  def listOrGetAction(viewName: String)(implicit user: User, state: ApplicationState, timeout: QueryTimeout) =
    parameterMultiMap { params =>
      val impliedIdForGetOpt = app.impliedIdForGetOverList(viewName)
      if (impliedIdForGetOpt.isDefined)
        if (useActions(viewName, Action.Get)) {
          extractRequest { implicit httpReq =>
            implicit val routeLogger: Logger = WabaseService.routeLogger(httpReq)
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
      extractRequest { implicit httpReq =>
        complete {
          implicit val routeLogger: Logger = WabaseService.routeLogger(httpReq)
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

  def postByKeyAction(viewName: String, keyValues: Seq[Any])(implicit user: User, state: ApplicationState, timeout: QueryTimeout) =
    extractUri { requestUri =>
      parameterMultiMap { params =>
        extractRequest { implicit httpReq =>
          onSuccess(app.checkApi(viewName, requestUri.path, ActionForHttpPost, user, keyValues)(
            AuthContext(actorSystem, httpReq, timeout, WabaseService.routeLogger(httpReq))
          )) { actionName =>
            if (useActions(viewName, actionName)) {
              entityAsMapOrException(viewName) { entityAsMap =>
                complete {
                  implicit val routeLogger: Logger = WabaseService.routeLogger(httpReq)
                  app.doWabaseAction(actionName, viewName, keyValues, filterPars(params), entityAsMap,
                    doApiCheck = false /* api checked above */)
                }
              }
            } else {
              entityAsMapOrException(viewName) { data =>
                val keyAsMap = if (keyValues.nonEmpty) app.prepareKey(viewName, keyValues, "insert") else Map.empty
                val id = app.save(viewName, data, filterPars(params) ++ keyAsMap)
                redirect(Uri(path = requestUri.path / id.toString), StatusCodes.SeeOther)
              }
            }
          }
        }
      }
    }

  def countAction(viewName: String)(implicit user: User, state: ApplicationState, timeout: QueryTimeout) =
    parameterMultiMap { params =>
      if (useActions(viewName, Action.Count)) {
        extractRequest { implicit httpReq =>
          implicit val routeLogger: Logger = WabaseService.routeLogger(httpReq)
          complete(app.doWabaseAction(Action.Count, viewName, Nil, filterPars(params)))
        }
      } else {
        complete(app.count(viewName, filterPars(params)).toString)
      }
    }

  def filterPars(params: Map[String, List[String]]) = {
    val decoded = AppServiceBase.decodeParams(metadataConventions, namesForInts, escapeReflectedXss)(params)
    decoded.get("filter").collect { case m: Map[String, Any]@unchecked => decoded - "filter" ++ m }.getOrElse(decoded)
  }

  @annotation.nowarn("cat=deprecation")
  def crudActionOnKeyInPath(implicit user: User) = applicationState { implicit state =>
    extractTimeout { implicit timeout =>
      getByIdPath     { getByIdAction     } ~
      countPath       { countAction       } ~
      createPath      { createAction      } ~
      deleteByIdPath  { deleteByIdAction  } ~
      deleteByKeyPath { deleteByKeyAction } ~
      putByIdPath     { putByIdAction     } ~
      putByKeyPath    { putByKeyAction    } ~
      listOrGetPath   { listOrGetAction   } ~
      getByKeyPath    { getByKeyAction    } ~
      postByKeyPath   { postByKeyAction   }
    }
  }

  def crudAction(implicit user: User) =
    pathPrefixTest(Segment ~ PathEnd) { _ =>
      mapRequestContext(maybeKeyFromQueryToPath) {
        crudActionOnKeyInPath
      }
    } ~ pathPrefixTest(!(Segment ~ PathEnd)) {
      crudActionOnKeyInPath
    }

  /** Whether resource keys may be supplied in the URI query string (?/key/parts).
    * Controlled by `app.key-in-query` (default true). Override in tests if needed. */
  def keyInQuery: Boolean = AppServiceBase.KeyInQuery

  /** Moves key from special query string (?/key/parts) into the path when [[keyInQuery]] is true. */
  def maybeKeyFromQueryToPath(context: RequestContext): RequestContext =
    if (keyInQuery) keyFromQueryToPath(context) else context

  /** Moves key from special query string (?/key/parts) into the path. */
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

  def apiAction(implicit user: User) = (extractRequest & extractTimeout) { (httpReq, timeout) =>
    complete(app.api(user)(AuthContext(actorSystem, httpReq, timeout, WabaseService.routeLogger(httpReq))))
  }
  def metadataAction(viewName: String)(implicit user: User, state: ApplicationState) =
    respondWithHeader(ETag(EntityTag(app.metadataVersionString))) {
      conditional(EntityTag(app.metadataVersionString), DateTime(app.startupTimeMillis)) {
        val obj = if (viewName == "*") app.apiMetadata else app.metadata(viewName)
        complete(obj)
      }
  }

  val DefaultResourceExtensions = config.getStringList("app.resource-extensions").asScala.toSet
  val DefaultResourcePathBase = config.getString("app.resource-path-base")
  def staticResources(extensions: Set[String] = DefaultResourceExtensions, basePath: String = DefaultResourcePathBase): Route =
    pathSuffixTest(new Regex(extensions.map("\\." + _).mkString(".*(", "|", ")$"))) { p =>
      path(Remaining) { resource =>
        (encodeResponseWith(NoCoding, Gzip, Deflate) & respondWithHeader(ETag(EntityTag(app.metadataVersionString)))) {
          getFromResource(basePath + "/" + resource)
        }
      }
    }
  def decodeParams(params: Map[String, List[String]]): Map[String, Any] =
    AppServiceBase.decodeParams(metadataConventions, namesForInts, escapeReflectedXss)(params)
  def decodeMultiParams(params: Map[String, List[String]]) =
    AppServiceBase.decodeMultiParams(metadataConventions, namesForInts, escapeReflectedXss)(params)
  val namesForInts = AppServiceBase.NamesForInts
  def escapeReflectedXss(msg: String) = AppServiceBase.escapeReflectedXss(msg)
  def decodeParam(key: String, value: String) =
    AppServiceBase.decodeParam(metadataConventions, namesForInts, escapeReflectedXss)(key, value)
  override def dbAccess = app.dbAccess

  protected def fileStreamerConfigs: Seq[AppFileStreamerConfig] = {
    Option(this)
      .collect { case fs: AppFileServiceBase[_] => fs.fileStreamer }
      .toList ++
      Option(this)
        .flatMap {
          case dc: DeferredControl => dc.deferredFileStreamerConfig
          case _ => None
        }
        .toList
  }
}

trait AppFileServiceBase[User] {
  this: AppProvider[User] with BasicJsonMarshalling
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
      val contentType = ctx.request.entity.contentType
      def multipartFormUpload = new Directive[Tuple3[Source[ByteString, Any], String, String]] {
        override def tapply(f: Tuple3[Source[ByteString, Any], String, String] => Route): Route = { ctx =>
          fileUpload("file").tapply { case Tuple1((fileInfo, bytes)) =>
              validateFileName(fileInfo.fileName)
              f(Tuple3(bytes, fileInfo.fileName, fileInfo.contentType.toString))
          }(ctx).flatMap {
            case rejected @ RouteResult.Rejected(rejs) if rejs.contains(MissingFormFieldRejection("file")) =>
              Future.failed(new BusinessException(s"No part named 'file' with 'filename' found in multipart upload"))
            case other =>
              Future.successful(other)
          }(ctx.executionContext)
        }
      }
      def simpleUpload(fileName: String) = {
        validateFileName(fileName)
        provide (ctx.request.entity.dataBytes) & provide(fileName) & provide(contentType.toString)
      }
      filenameOpt match {
        case None =>
          contentType match {
            case multipartFormData if WabaseUnmarshallers.isMultipartFormData(multipartFormData.mediaType) =>
              multipartFormUpload
            case _ =>
              simpleUpload("file")
          }
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
    (uploadMultipleAsSource & extractRequestContext).tflatMap { case (src, ctx) =>
      provide {
        import ctx._
        src.runFold(Seq.empty[PartInfo])(_ :+ _)
      }
    }
  }

  def uploadMultipleAsSource(implicit
        user: User,
        state: ApplicationState
    ): Directive1[Source[PartInfo, _]] = {
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
        }
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
  val ApplicationStateCookiePrefix = config.getString("app.state-cookie-prefix")
  val NamesForInts = config.getStringList("app.names-for-int-params").asScala.toSet
  /** When true, resource keys use URI query form (?/key/parts) for incoming requests and outgoing uris.
    * Defaults to true if `app.key-in-query` is unset. See also [[TresqlUri.keyInQuery]]. */
  val KeyInQuery: Boolean =
    Option("app.key-in-query").filter(config.hasPath).forall(config.getBoolean)

  def escapeReflectedXss(msg: String) =
    msg.replace("<", "[<]")

  def decodeParam(
    metadataConventions: AppMetadata.AppMdConventions,
    namesForInts: Set[String],
    escapeReflectedXss: String => String,
  )(
    key: String, value: String) = {
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
      handleType(d => Format.convertToType(d.replaceAll("\"", ""), ClassOfJavaSqlDate), "date")
    } else if (metadataConventions.isDateTimeName(key)) {
      handleType(d => Format.convertToType(d.replaceAll("\"", ""), ClassOfJavaSqlTimestamp), "dateTime (not supported yet)")
    } else if (namesForInts.contains(key) ||
      metadataConventions.isIntegerName(key) ||
      metadataConventions.isIdName(key) ||
      metadataConventions.isIdRefName(key)) {
      handleType(l => java.lang.Long.valueOf(l), "long")
    } else if (metadataConventions.isDecimalName(key)) {
      handleType(d => BigDecimal(d), "bigDecimal")
    } else value
  }

  def decodeParams(
    metadataConventions: AppMetadata.AppMdConventions,
    namesForInts: Set[String],
    escapeReflectedXss: String => String,
  )(
    params: Map[String, List[String]]): Map[String, Any] = params map { t =>
    t._1 -> (t._2.map(decodeParam(metadataConventions, namesForInts, escapeReflectedXss)(t._1, _)) match {
      case List(x) => x
      case x @ List(_, _*) => x
      case x => throw new IllegalStateException("unexpected: " + x)
    })
  }
  def decodeMultiParams(
    metadataConventions: AppMetadata.AppMdConventions,
    namesForInts: Set[String],
    escapeReflectedXss: String => String,
  )(params: Map[String, List[String]]): Map[String, List[Any]] =
    params map { t => t._1 -> t._2.map(decodeParam(metadataConventions, namesForInts, escapeReflectedXss)(t._1, _)) }

  def filterParams(
    metadataConventions: AppMetadata.AppMdConventions,
    namesForInts: Set[String],
    escapeReflectedXss: String => String,
    parametersDecoder: HttpRequest => Map[String, Any],
    httpRequest: HttpRequest,
  ): Map[String, Any] = {
    def defaultDecode =
      decodeParams(metadataConventions, namesForInts, escapeReflectedXss)(WabaseService.parameterMultiMap(httpRequest))
    if (parametersDecoder != null) parametersDecoder(httpRequest) match {
      case null => defaultDecode
      case x => x
    } else defaultDecode
  }

  trait AppStateExtractor { this: AppServiceBase[_] with QueryTimeoutExtractor with Execution =>
    val ApplicationStateCookiePrefix = AppServiceBase.ApplicationStateCookiePrefix
    def applicationState = extract(r => extractState(r.request, ApplicationStateCookiePrefix))
    protected def extractState(req: HttpRequest, prefix: String) = {
      val state = req.headers.flatMap {
        case c: Cookie => c.cookies.filter(_.name.startsWith(prefix))
        case _ => Nil
      } map (c => c.name -> this.decodeParam(c.name, c.value)) toMap
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
    def appExceptionHandler: ExceptionHandler
  }

  object AppExceptionHandler{
    def entityStreamSizeExceptionHandler(marshalling: BasicJsonMarshalling) = ExceptionHandler {
      case e: EntityStreamSizeException =>
        import marshalling._
        val response = Map[String, Any]("actualSize"-> e.actualSize.orNull, "limit" -> e.limit)
        complete(StatusCodes.ContentTooLarge -> response)
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
        complete(HttpResponse(UnprocessableContent, entity = e.getMessage))
    }

    def bindVariableExceptionHandler(logger: com.typesafe.scalalogging.Logger,
        bindVariableExceptionResponseMessage: MissingBindVariableException => String = _.getMessage) = ExceptionHandler {
      case e: MissingBindVariableException =>
        logger.debug(e.getMessage, e)
        complete(HttpResponse(BadRequest, entity = bindVariableExceptionResponseMessage(e)))
    }

    def httpExceptionHandler(logger: com.typesafe.scalalogging.Logger) = ExceptionHandler {
      case e: HttpException =>
        logger.debug(e.getMessage, e)
        complete(HttpResponse(status = e.status, entity = e.getMessage))
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

    def validationExceptionPathsHandler(logger: com.typesafe.scalalogging.Logger) = ExceptionHandler {
      case e: ValidationException =>
        logger.trace(e.getMessage, e)
        import io.bullet.borer._, io.bullet.borer.derivation.MapBasedCodecs._, ResultEncoder._, JsonEncoder._
        implicit val enc = deriveEncoder[ValidationResult]
        complete(HttpResponse(BadRequest, entity = Json.encode(e.details).toUtf8String))
    }

    def csrfExceptionHandler = {
      val logger = LoggerFactory.getLogger("org.wabase.csrf")
      ExceptionHandler {
        case e: CSRFException =>
          logger.info(e.toString)
          complete(StatusCodes.BadRequest)
      }
    }

    def quereaseActionExceptionHandler(innerHandler: => ExceptionHandler) = ExceptionHandler {
      case e: QuereaseActionException => innerHandler(e.getCause)
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
      override lazy val appExceptionHandler = PostgresTimeoutExceptionHandler(this)
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
      override lazy val appExceptionHandler =
        unprocessableEntityExceptionHandler(this.logger)
          .withFallback(httpExceptionHandler(this.logger))
          .withFallback(businessExceptionHandler(this.logger))
          .withFallback(bindVariableExceptionHandler(this.logger, this.bindVariableExceptionResponseMessage))
          .withFallback(quereaseEnvExceptionHandler(this.logger))
          .withFallback(viewNotFoundExceptionHandler)
          .withFallback(rowNotFoundExceptionHandler)
          .withFallback(csrfExceptionHandler)
          .withFallback(quereaseActionExceptionHandler(appExceptionHandler))
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
      override lazy val appExceptionHandler =
        unprocessableEntityExceptionHandler(this.logger)
          .withFallback(httpExceptionHandler(this.logger))
          .withFallback(businessExceptionHandler(this.logger))
          .withFallback(validationExceptionHandler(this.logger))
          .withFallback(entityStreamSizeExceptionHandler(this))
          .withFallback(bindVariableExceptionHandler(this.logger, this.bindVariableExceptionResponseMessage))
          .withFallback(quereaseEnvExceptionHandler(this.logger))
          .withFallback(PostgresTimeoutExceptionHandler(this))
          .withFallback(TresqExceptionHandler(this))
          .withFallback(viewNotFoundExceptionHandler)
          .withFallback(rowNotFoundExceptionHandler)
          .withFallback(csrfExceptionHandler)
          .withFallback(quereaseActionExceptionHandler(appExceptionHandler))
    }
  }

  trait AppI18nService { this: AppServiceBase[_] =>
    val ApplicationLanguageCookiePostfix = I18nService.ApplicationLanguageCookiePostfix

    val i18n: I18n = initI18n
    protected def initI18n: I18n = app

    def i18nPath = pathPrefix("i18n") & get
    def i18nLanguagePath = path("lang" / Segment)
    def i18nResourcePath = i18nPath & path(Segment ~ Slash.?)
    def i18nTranslatePath = i18nPath & path(Segment / Segment / RemainingPath ~ Slash.?)

    protected def langCookieTransformer(cookie: HttpCookie): HttpCookie = cookie

    def setLanguage: Route = (i18nPath & i18nLanguagePath) { lang =>
      setCookie(langCookieTransformer(
        HttpCookie(this.ApplicationStateCookiePrefix + this.ApplicationLanguageCookiePostfix,
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
        complete(i18n.translateFromBundle(name, key, WabaseService.pathSegments(params): _*))
      }
    }

    def currentLangFromHeader(request: HttpRequest) = I18nService.currentLangFromHeader(request)

    def applicationLocale = applicationState.map(getApplicationLocale)

    def getApplicationLocale(state: ApplicationState): Locale =
      state.state.get(this.ApplicationStateCookiePrefix + this.ApplicationLanguageCookiePostfix)
        .map(l => new Locale(String.valueOf(l)))
        .getOrElse(Locale.getDefault)

    implicit def i18BundleMarshaller: ToEntityMarshaller[I18Bundle] = I18nService.i18BundleMarshaller
  }
}
