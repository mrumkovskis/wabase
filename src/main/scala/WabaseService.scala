package org.wabase

import org.apache.pekko.http.scaladsl.model.Uri.Path
import org.apache.pekko.http.scaladsl.model.Uri.Path.{Empty, Segment, SlashOrEmpty}
import AppMetadata._
import com.typesafe.scalalogging.Logger
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.marshalling.ToResponseMarshallable
import org.apache.pekko.http.scaladsl.model.headers.{Allow, Cookie, EntityTag, HttpCookie, `Set-Cookie`, `Timeout-Access`}
import org.apache.pekko.http.scaladsl.model.HttpCharsets.`UTF-8`
import org.apache.pekko.http.scaladsl.model.{ContentType, ContentTypes, DateTime, HttpEntity, HttpHeader, HttpMessage, HttpMethod, HttpRequest, HttpResponse, StatusCode, StatusCodes, Uri, MediaType => PekkoMediaType}
import org.apache.pekko.http.scaladsl.unmarshalling.Unmarshaller
import org.mojoz.metadata.ViewDef
import org.slf4j.LoggerFactory
import org.tresql.parsing.QueryParsers
import org.wabase.AppMetadata.{Action, RouteDef}
import org.wabase.swagger.WabaseSwaggerGenerator
import org.wabase.WabaseService.Wabase
import org.wabase.ds.QueryTimeout

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.control.NonFatal
import scala.util.parsing.input.CharSequenceReader

case class WabaseUser(properties: Map[String, Any]) {
  val id: Long      = properties.get("id").collect {
    case x: Number => x.longValue
    case s: String => Try(s.toLong).getOrElse(-1L)
  }.getOrElse(-1)
  val name: String  = properties.get("name").map(n => Option(n).map(String.valueOf).orNull)
    .orElse(Option(id).filter(_ != -1).map(_.toString)).orNull
  val roles: Set[String] = properties.get("roles")
    .collect { case r: Iterable[String@unchecked] => r.toSet }.getOrElse(Set())
}

case class WabaseRequestContext(
  wabase: Wabase,
  req: HttpRequest,
  deferred: Deferred = null,
  route: RouteDef = null,
  viewName: String = null,
  action: String = null,
  key: Seq[Any] = Nil,
  applicationState: ApplicationState = null,
  user: WabaseUser = null,
  queryTimeout: QueryTimeout = null,
  as: ActorSystem = null,
  resultFilter: ResultRenderer.ResultFilter = null,
  logger: Logger = null,
) {
  def withResultFilter(resFil: ResultRenderer.ResultFilter): WabaseRequestContext =
    copy(resultFilter = resFil)
}

case class Deferred(
  deferredControl: WabaseDeferredControl = null,
  deferredModule: String = WabaseDeferredControl.defaultModuleId
)

class HttpException(val status: StatusCode, message: String) extends Exception(message)
object HttpException {
  def apply(status: StatusCode): HttpException = new HttpException(status, status.reason)
}

class WabaseService {
  def handle(
    wabase: Wabase,
    deferredControl: WabaseDeferredControl,
  )(req: HttpRequest)(implicit as: ActorSystem): Future[HttpResponse] = {
    WabaseService.handle(wabase, deferredControl)(req)
  }
}

object WabaseService extends Loggable {

  object MediaTypes {
    val `application/json`: PekkoMediaType.WithFixedCharset =
      org.apache.pekko.http.scaladsl.model.MediaTypes.`application/json`
    val `application/yaml`: PekkoMediaType.WithFixedCharset =
      PekkoMediaType.applicationWithFixedCharset("yaml", `UTF-8`, "yaml")
  }

  type RequestHandler = WabaseRequestContext => Future[HttpResponse]
  type ErrorHandler   = PartialFunction[Throwable, Future[HttpResponse]]
  type Wabase = WabaseApp[WabaseUser]
    with Authorization[WabaseUser]
    with QuereaseProvider
    with I18n
    with DbAccess
    with Marshalling
    with AppProvider[WabaseUser]
    with Execution

  def handle(
    wabase: Wabase,
    deferredControl: WabaseDeferredControl,
  )(req: HttpRequest)(implicit as: ActorSystem): Future[HttpResponse] = {
    val ctx = WabaseRequestContext(wabase, req, Deferred(deferredControl = deferredControl), as = as)
    logger.debug(s"Matching route for path: ${req.uri.path}")
    findRoute(ctx).fold(
      resp => {
        logger.debug(s"Route not found for path: ${req.uri.path}, error code: ${resp.status}")
        Future.successful(resp)
      },
      route => {
        // route logger
        val ctxWithLogger = ctx.copy(logger = routeLogger(ctx.req))
        ctxWithLogger.logger
          .debug(s"Route '${route.methods.map(_.value + " ").mkString}${route.path}' matched for request '${
            ctxWithLogger.req.method.value} ${ctxWithLogger.req.uri}'")
        doRoute(ctxWithLogger.copy(route = route))
      }
    )
  }

  private val LoggerNameFactory =
    getObjectOrNewInstance(config.getString("app.wabase-logger-name-factory"), "logger name factory")
      .asInstanceOf[LoggerNameFactory]
  def routeLogger(req: HttpRequest): Logger = {
    Logger(LoggerFactory.getLogger(LoggerNameFactory.loggerName(req)))
  }

  private val swaggerGeneratorFactory =
    getObjectOrNewInstance(config.getString("app.wabase-swagger-generator-factory"), "swagger generator factory")
      .asInstanceOf[WabaseSwaggerGeneratorFactory]
  def createSwaggerGenerator(ctx: WabaseRequestContext) =
    swaggerGeneratorFactory.createSwaggerGenerator(ctx)

  /* If route found return Right(route) else Left(http client error) */
  def findRoute(ctx: WabaseRequestContext): Either[HttpResponse, RouteDef] = {
    val pathString = WabaseService.toReadableString(ctx.req.uri.path)
    val method = ctx.req.method
    // Collect allowed methods for path matches so 405 responses can include a proper Allow header
    // (RFC 9110 §15.5.6 requires Allow on Method Not Allowed).
    var allowedMethods: Set[HttpMethod] = Set.empty
    ctx.wabase.qe.routeDefs.find { rd =>
        rd.path.pattern.matcher(pathString).matches &&
          (rd.methods.isEmpty || rd.methods(method) || {
            allowedMethods ++= rd.methods
            false
          })
      }.map(Right[HttpResponse, RouteDef])
      .getOrElse {
        if (allowedMethods.nonEmpty) Left(methodNotAllowed(allowedMethods))
        else Left(notFound)
      }
  }

  /** 405 response with Allow header listing methods currently supported for the resource. */
  def methodNotAllowed(allowedMethods: Iterable[HttpMethod]): HttpResponse =
    HttpResponse(
      status = StatusCodes.MethodNotAllowed,
      headers = List(Allow(allowedMethods.toList.sortBy(_.value))),
    )

  def doRoute(ctx: WabaseRequestContext)(implicit as: ActorSystem): Future[HttpResponse] = {
    implicit val ec: ExecutionContext = as.dispatcher
    val errorHandler = WabaseService.sealedErrorHandler(WabaseService.errorHandler(ctx))(ctx)
    try {
      val handler = buildRequestHandlerChain(ctx.route.requestHandler, null)
      handler(ctx).recoverWith(errorHandler)
    } catch { case NonFatal(e) => errorHandler(e) }
  }

  val notFound: HttpResponse = HttpResponse(status = StatusCodes.NotFound)

  def optionalHttpHeaderValue[T](msg: HttpMessage)(extractorF: HttpHeader => Option[T]): Option[T] = {
    msg.headers.collectFirst(Function.unlift(extractorF))
  }

  def optionalHttpHeaderValueByName(msg: HttpMessage)(name: String): Option[String] = {
    optionalHttpHeaderValue(msg)(optionalHttpHeaderValueExtractor(name.toLowerCase))
  }

  def optionalHttpHeaderValuePF[T](msg: HttpMessage)(extractorPF: PartialFunction[HttpHeader, T]): Option[T] = {
    optionalHttpHeaderValue(msg)(extractorPF.lift)
  }

  def optionalHttpHeaderValueExtractor(lowerCaseName: String): HttpHeader => Option[String] = {
    case h: HttpHeader if h.is(lowerCaseName) => Some(h.value)
    case _                                    => None
  }

  def optionalCookie(req: HttpRequest)(name: String): Option[String] = {
    optionalHttpHeaderValue(req)({
      case Cookie(cookies) => cookies.find(_.name == name).map(_.value)
      case _               => None
    })
  }

  def parameterMultiMap(req: HttpRequest): Map[String, List[String]] = req.uri.query().toMultiMap

  def setCookie(resp: HttpResponse)(first: HttpCookie, more: HttpCookie*): HttpResponse = {
    resp.mapHeaders(_ ++ (first :: more.toList).map(`Set-Cookie`(_)))
  }

  def deleteCookie(resp: HttpResponse)(name: String, domain: String = "", path: String = ""): HttpResponse = {
    val cookie = HttpCookie(name, "",
      domain = Option(domain).filter(_.nonEmpty), path = Option(path).filter(_.nonEmpty))
    resp.mapHeaders(_ ++ Seq(`Set-Cookie`(cookie.withValue("").withExpires(DateTime.MinValue))))
  }

  def pathSegments(path: Path): List[String] = path match {
    case Path.Empty => Nil
    case _: Path.Slash => pathSegments(path.tail)
    case Path.Segment(h, t) => h :: pathSegments(t)
  }

  def complete(ctx: WabaseRequestContext, marshallable: => ToResponseMarshallable): Future[HttpResponse] =
    marshallable(ctx.req)(ctx.as.dispatcher)

  /** Utility function. Extracts Content-Type from list of headers since pekko renders content type from http entity not from header list */
  def partitionHeaders(headers: List[HttpHeader]): (Option[ContentType], List[HttpHeader]) = {
    headers.partition(_.is("content-type")) match {
      case (cts, h) => cts.map(cth => ContentType.parse(cth.value)).collectFirst {
        case Right(ct) => ct
        case Left(errs) => throw new IllegalArgumentException(s"Error(s) parsing content type:\n${
          errs.map(_.formatPretty).mkString("\n")
        }")
      } -> h
    }
  }

  def pathMatchedGroups(ctx: WabaseRequestContext): Option[List[String]] = {
    ctx.route.path.unapplySeq(toReadableString(ctx.req.uri.path))
  }

  def conditionsFor(length: Long, lastModified: Long): (Option[EntityTag], Option[DateTime]) = {
    // extractSettings.flatMap(settings =>
      // if (settings.fileGetConditional) {
        val tag = java.lang.Long.toHexString(lastModified ^ java.lang.Long.reverse(length))
        val lastModifiedDateTime = DateTime(math.min(lastModified, System.currentTimeMillis))
        (Some(EntityTag(tag)), Some(lastModifiedDateTime))
      // } else (None, None))
  }

  /** Extract segments as list from path after segment matching prefix */
  def key(path: Path, prefix: String): Seq[String] = {
    def key_path(path: Path): Path = path match {
      case Segment(head, tail) =>
        if (head == prefix) tail
        else key_path(tail)
      case Empty => Empty
      case p => key_path(p.tail)
    }
    val keyPath = key_path(path)
    def key(path: Path): List[String] = path match {
      case Segment(v, tail) => v :: key(tail)
      case Empty => Nil
      case p: SlashOrEmpty => key(p.tail)
    }
    key(keyPath)
  }

  def addResultFilter(context: WabaseRequestContext, params: Map[String, Any]): WabaseRequestContext = {
    if (context.resultFilter != null) context
    else Option(context.wabase.createResultFilter(context.action, context.viewName, params)(context.logger))
      .map(context.withResultFilter)
      .getOrElse(context)
  }

  def withReqMaxContentSize(ctx: WabaseRequestContext): WabaseRequestContext = {
    if (ctx.viewName == null) ctx else {
      val vd = ctx.wabase.qe.viewDef(ctx.viewName)
      if (vd.maxContentSize == null) ctx else {
        val req = ctx.req.withEntity(ctx.req.entity.withSizeLimit(vd.maxContentSize))
        ctx.copy(req = req)
      }
    }
  }

  def withReqTimeout(ctx: WabaseRequestContext): WabaseRequestContext = {
    if (ctx.viewName == null) ctx else {
      val vd = ctx.wabase.qe.viewDef(ctx.viewName)
      if(vd.timeout == null) ctx else {
        ctx.req.header[`Timeout-Access`].map(_.timeoutAccess.updateTimeout(vd.timeout))
          .getOrElse(ctx.logger.warn(s"request timeout is defined for view ${vd.name}, however no request-timeout http header is set!"))
        ctx
      }
    }
  }

  val BodyActions: Set[String] =
    Set(Action.Insert, Action.Update, Action.UpdatePlus, Action.Upsert, Action.Save, Action.Post, Action.Put)

  def toMapForViewEntityDecoder(ctx: WabaseRequestContext): Future[Map[String, Any]] = {
    import ctx._
    implicit val system: ActorSystem = as
    implicit val ec: ExecutionContext = as.dispatcher
    val vd = wabase.qe.viewDef(viewName)
    vd.decoder match {
      case AppMetadata.DefaultDecoder if BodyActions.contains(action) =>
        def defaultContent = wabase.toMapUnmarshallerForView(viewName)(req.entity)
        def mappedContent  = wabase.toMapUnmarshaller(req.entity).map(m => wabase.qe.toCompatibleMap(m, vd))
        req.entity.contentType match {
          case ContentTypes.`application/json` => defaultContent
          case ContentTypes.`application/x-www-form-urlencoded` =>
            mappedContent
          case multipartFormData if WabaseUnmarshallers.isMultipartFormData(multipartFormData.mediaType) =>
            mappedContent
          case _ => defaultContent
        }
      case AppMetadata.CustomDecoder(o, f) if BodyActions.contains(action) =>
        invokeFunction(o, f,
          Seq[(Class[_], () => Any)](
            (classOf[HttpRequest], () => req),
            (classOf[ActorSystem], () => as),
            (classOf[ExecutionContext], () => ec)
          )
        ) match {
          case f: Future[_] => f.mapTo[Map[String, Any]]
          case m: Map[String, Any]@unchecked => Future.successful(m)
          case x => throw new IllegalArgumentException(s"Custom decoder must return Map[String, Any], instead got: $x")
        }
      case AppMetadata.NoneDecoder => Future.successful(Map())
      case _ => req.entity.discardBytes().future.map(_ => Map())
    }
  }

  def toStringEntityDecoder(ctx: WabaseRequestContext): Future[String] = {
    import ctx._
    implicit val system: ActorSystem = as
    implicit val ec: ExecutionContext = as.dispatcher
    Unmarshaller.stringUnmarshaller(req.entity)
  }

  def toMapEntityDecoder(ctx: WabaseRequestContext): Future[Map[String, Any]] =
    toAnyEntityDecoder(ctx).mapTo[Map[String, Any]]

  def toSeqEntityDecoder(ctx: WabaseRequestContext): Future[Seq[Any]] =
    toAnyEntityDecoder(ctx).mapTo[Seq[Any]]

  private def toAnyEntityDecoder(ctx: WabaseRequestContext): Future[Any] = {
    import ctx._
    implicit val system: ActorSystem = as
    implicit val ec: ExecutionContext = as.dispatcher
    def decodeJs = Unmarshaller.byteStringUnmarshaller
      .map { b => CborOrJsonAnyValueDecoder.decode(b) }
    req.entity.contentType match {
      case ContentTypes.`application/json` => decodeJs(req.entity)
      case ContentTypes.`application/x-www-form-urlencoded` =>
        wabase.toMapUnmarshaller(req.entity)
      case multipartFormData if WabaseUnmarshallers.isMultipartFormData(multipartFormData.mediaType) =>
        wabase.toMapUnmarshaller(req.entity)
      case _ => decodeJs(req.entity)
    }
  }

  def toReadableString(path: Path): String = {
    @tailrec def trs(p: Path, sb: StringBuilder): String = p match {
      case Path.Empty => sb.toString
      case _: Path => trs(p.tail, sb.append(p.head))
    }
    trs(path, new StringBuilder())
  }

  def buildRequestHandlerChain(inv: Action.Invocation, innerHandler: RequestHandler): RequestHandler = {
    inv.args match {
      case inv_args => inv_args.splitAt(inv_args.size - 1) match {
        case (args, List(innerInv: Action.Invocation)) =>
          buildRequestHandler(inv.className, inv.function, HandlerArgsParser.argValues(args),
            buildRequestHandlerChain(innerInv, innerHandler))
        case _ =>
          buildRequestHandler(inv.className, inv.function, HandlerArgsParser.argValues(inv_args), innerHandler)
      }
    }
  }

  def buildRequestHandler(cn: String, fn: String,
                          invocationArgs: List[HandlerArgsParser.HandlerArg],
                          ih: RequestHandler): RequestHandler = wrc => {
    wrc.logger.debug(s"Invoking handler $cn.$fn for request: ${wrc.req}")
    implicit val ec: ExecutionContext = wrc.as.dispatcher
    def missingHandlerError = sys.error(s"Handler argument missing for invocation: '$cn.$fn'")
    val (paramList, paramFunction) = handlerParameters(wrc, ih, invocationArgs, missingHandlerError)
    val result = invokeFunction(cn, fn, paramList, paramFunction)
    handlerResult(wrc, result).flatMap {
      case c: WabaseRequestContext => if (ih == null) missingHandlerError else ih(c)
      case r: HttpResponse => Future.successful(r)
      case h: RequestHandler@unchecked => h(wrc)
    }
  }

  def handlerParameters(
    wrc: WabaseRequestContext,
    innerHandler: RequestHandler,
    invocationArgs: List[HandlerArgsParser.HandlerArg],
    missingHandlerError: => Nothing,
  ): (Seq[(Class[_], () => Any)], InvocationParameterFun) = {
    implicit val ec: ExecutionContext = wrc.as.dispatcher
    val paramList = List(
      (classOf[WabaseRequestContext], () => wrc),
      (classOf[HttpRequest], () => wrc.req),
      (classOf[HttpResponse], () => if (innerHandler == null) missingHandlerError else innerHandler(wrc)),
      (classOf[Future[HttpResponse]], () => if (innerHandler == null) missingHandlerError else innerHandler(wrc)),
      (classOf[ActorSystem], () => wrc.as),
      (classOf[ExecutionContext], () => ec),
      (classOf[RequestHandler], () => innerHandler),
      (classOf[WabaseUser], () => wrc.user),
      (classOf[ApplicationState], () => wrc.applicationState),
      (classOf[Uri], () => wrc.req.uri),
      (classOf[Map[String, Any]], () => toMapEntityDecoder(wrc)), // map is function so it comes after request handler
      (classOf[Seq[Any]], () => toSeqEntityDecoder(wrc)), // seq is function so it comes after request handler
      (classOf[java.util.Map[_, _]], () => toMapEntityDecoder(wrc).map(_.asJava)),
      (classOf[java.util.List[_]], () => toSeqEntityDecoder(wrc).map(_.asJava)),
      (classOf[String], () => toStringEntityDecoder(wrc)),
    )

    val paramFunction = invocationArgs.map {
      case HandlerArgsParser.StringArg(s) => s
      case HandlerArgsParser.RegexGroupRef(nr) =>
        pathMatchedGroups(wrc).flatMap(_.lift(nr - 1))
          .getOrElse(sys.error(
              s"Group nr '$nr' not found in route '${wrc.route.path}' for path '${wrc.req.uri.path}'"))
      case HandlerArgsParser.NumberArg(n) => n
    }.zipWithIndex.map { case (value, idx) =>
      { case (par, i) if idx == i => wrc.wabase.qe.convertToType(value, par.getType) }:InvocationParameterFun
    }.foldLeft(PartialFunction.empty[InvocationParameter, Any])(_ orElse _) orElse
      AppQuerease.dtoParameterFromMapF(() => toMapEntityDecoder(wrc))(wrc.wabase.qio)

    (paramList, paramFunction)
  }

  def handlerResult(wrc: WabaseRequestContext, res: Any): Future[Any] = {
    implicit val ec: ExecutionContext = wrc.as.dispatcher
    def processResult(r: Any): Future[Any] = r match {
      case c: WabaseRequestContext => Future.successful(c)
      case req: HttpRequest => processResult(wrc.copy(req = req))
      case resp: HttpResponse => Future.successful(resp)
      case f: Future[_] => f.flatMap(processResult)
      case s: String => Future.successful(HttpResponse(entity = s))
      case _: Map[_, _] | _: scala.collection.mutable.Map[_, _] => Future.successful(jsonResponse(r))
      case s: Iterable[_] => Future.successful(jsonResponse(s.map { case e: Dto => e.toMap(wrc.wabase.qe) case x => x }))
      case rh: RequestHandler@unchecked => Future.successful(rh)
      case d: Dto => Future.successful(jsonResponse(d.toMap(wrc.wabase.qe)))
      case m: java.util.Map[_, _] => processResult(m.asScala)
      case s: java.util.List[_] => processResult(s.asScala)
      case uri: Uri => processResult(wrc.copy(req = wrc.req.withUri(uri)))
      case st: ApplicationState => processResult(wrc.copy(applicationState = st))
      case u: WabaseUser => processResult(wrc.copy(user = u))
      case o: Option[_] => o.map(processResult).getOrElse(Future.successful(HttpResponse(StatusCodes.NotFound)))
      case x => sys.error(s"Request transformer must return either WabaseRequestContext or HttpRequest or Future of them." +
        s" Instead got: $x")
    }
    processResult(res)
  }

  def jsonResponse(resp: Any): HttpResponse =
    HttpResponse(entity = HttpEntity(MediaTypes.`application/json`, ResultEncoder.encodeAnyToJsonString(resp)))

  private val ERR_AND_THEN_PARAM = "app.wabase-error-handler-and-then"
  private val error_and_then_cn_fn =
    if (!config.getIsNull(ERR_AND_THEN_PARAM)) config.getString(ERR_AND_THEN_PARAM)
    else null
  def errorHandler(wrc: WabaseRequestContext): ErrorHandler = {
    // for performance reasons do not create real error handler unless error occurs
    lazy val handler: ErrorHandler = {
      implicit val ec: ExecutionContext = wrc.as.dispatcher
      val eh = wrc.route.errorHandler
      val errorHandler = invokeFunction(
        eh.className, eh.function, Seq((classOf[WabaseRequestContext], () => wrc))
      ) match {
        case h: ErrorHandler@unchecked => h
        case x => sys.error(s"Error handler for route ${wrc.route.path} must return value of type:" +
          s" WabaseService.ErrorHandler, instead got '$x' of type '${x.getClass}'")
      }
      if (error_and_then_cn_fn == null) errorHandler
      else {
        val andThen = invokeFunction(error_and_then_cn_fn, Seq((classOf[WabaseRequestContext], () => wrc))) match {
          case f: Function[HttpResponse, Future[HttpResponse]]@unchecked => f
          case x => sys.error(s"Error handler and then function must return value of type:" +
            s" HttpResponse => Future[HttpResponse], instead got '$x' of type '${x.getClass}'")
        }
        errorHandler.andThen(_.flatMap(andThen))
      }
    }
    { case e if handler.isDefinedAt(e) => handler(e) }
  }

  def sealedErrorHandler(eh: ErrorHandler)(wrc: WabaseRequestContext): ErrorHandler = {
    eh orElse ({ case NonFatal(e) =>
      wrc.logger.error(s"[${WabaseErrorHandler.ctxDebugInfo(wrc)}] Internal server error, sending http 500", e)
      Future.successful(HttpResponse(status = StatusCodes.InternalServerError))
    }: ErrorHandler)
  }

  def error(status: StatusCode, msg: String) = throw new HttpException(status, msg)
}

object HandlerArgsParser extends QueryParsers {
  trait HandlerArg
  case class RegexGroupRef(nr: Int) extends HandlerArg
  case class StringArg(str: String) extends HandlerArg
  case class NumberArg(value: Long) extends HandlerArg
  def groupRef: MemParser[RegexGroupRef] = "\\$(\\d+)".r ^^ {
    gr => RegexGroupRef(gr.substring(1).toInt)
  } named "regex-group-arg"
  def nullArg: MemParser[StringArg] = "null" ^^ (_ => StringArg(null)) named "null-arg"
  def stringArg: MemParser[StringArg] = stringLiteral ^^ StringArg.apply named "string-arg"
  def numberArg: MemParser[NumberArg] = "\\d+".r ^^ (v => NumberArg(v.toLong)) named "number-arg"
  def arg: MemParser[HandlerArg] = (stringArg | groupRef | numberArg | nullArg)
    .withFailureMessage("Handler argument must be either string literal or regexp group ref - $<group nr> or long number.")
  def parsArg(value: String): HandlerArg = {
    phrase(arg)(new CharSequenceReader(value)) match {
      case Success(r, _) => r
      case x => sys.error(x.toString)
    }
  }
  def argValues(args: List[Action.Op]): List[HandlerArg] = args.map {
    case t: Action.Tresql => parsArg(t.tresql)
    case x => sys.error(s"Invalid handler arg: '$x'. Only string constants or regexp group refs allowed")
  }
}

trait LoggerNameFactory {
  def loggerName(req: HttpRequest): String
}

object LoggerNameFactory extends LoggerNameFactory {
  def loggerName(req: HttpRequest): String = {
    req.method.value.toLowerCase + WabaseService.toReadableString(req.uri.path).replace('/', '.')
  }
}

trait WabaseSwaggerGeneratorFactory {
  def createSwaggerGenerator(ctx: WabaseRequestContext): WabaseSwaggerGenerator
}

object WabaseSwaggerGeneratorFactory extends WabaseSwaggerGeneratorFactory {
  class WabaseDefaultSwaggerGenerator(ctx: WabaseRequestContext)
      extends WabaseSwaggerGenerator(Seq(ctx.wabase.qe), config.getString("app.host"),
        ctx.wabase.app.hasApiSync(_, null, _, _, _ => Future.successful(true))(ctx.as.dispatcher)) {
    override def getQueryParameters(method: String, viewDef: ViewDef, keySize: Int = 99): Seq[FilterParameter] = {
      super.getQueryParameters(method, viewDef, keySize)
        .filterNot(p => ctx.wabase.app.isInternalParameter(viewDef, p.name))
    }
  }
  override def createSwaggerGenerator(ctx: WabaseRequestContext): WabaseSwaggerGenerator = new WabaseDefaultSwaggerGenerator(ctx)
}
