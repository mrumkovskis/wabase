package org.wabase

import org.apache.pekko.http.scaladsl.model.HttpMethods._
import org.apache.pekko.http.scaladsl.model.Uri.Path
import org.apache.pekko.http.scaladsl.model.Uri.Path.{Empty, Segment, SlashOrEmpty}
import org.mojoz.metadata.ViewDef
import AppMetadata._
import org.apache.pekko.http.scaladsl.model.headers.{Cookie, HttpCookie, `Set-Cookie`}
import org.apache.pekko.http.scaladsl.model.{DateTime, HttpHeader, HttpRequest, HttpResponse}
import org.wabase.AppMetadata.{Action, RouteDef}

import scala.concurrent.{ExecutionContext, Future}

case class WabaseUser(properties: Map[String, Any]) {
  val id: Long      = properties.get("id").collect { case x: Number => x.longValue }.getOrElse(-1)
  val name: String  = properties.get("name").map(String.valueOf).orNull
}

case class WabaseRequestContext(
  route: RouteDef,
  viewDefs: Map[String, ViewDef],
  req: HttpRequest,
  viewName: String = null,
  action: String = null,
  key: Seq[Any] = Nil,
  applicationState: ApplicationState = null,
  user: WabaseUser = null,
)

class WabaseRouteException(message: String) extends Exception(message)

class WabaseService {

  private val CreateCountActionAndView = """(?U)(?:(count|create):)?(\w*)""".r

  def handle(
    routes: Seq[RouteDef],
    viewDefs: Map[String, ViewDef]
  )(req: HttpRequest)(implicit ec: ExecutionContext): Future[HttpResponse] = {
    val ctx = requestContext(routes, viewDefs)(req)
    doRoute(ctx)
  }

  protected def requestContext(
    routes: Seq[RouteDef],
    viewDefs: Map[String, ViewDef]
  )(req: HttpRequest): WabaseRequestContext = {
    val pathString = req.uri.path.toString
    val route = routes.find(_.path.pattern.matcher(pathString).matches)
      .getOrElse(error(s"Route not found for path '$pathString'"))
    WabaseRequestContext(route, viewDefs, req)
  }

  protected def doRoute(ctx: WabaseRequestContext)(implicit ec: ExecutionContext): Future[HttpResponse] = {
    def viewActionKey(ctx: WabaseRequestContext): WabaseRequestContext = {
      import ctx._
      val pathString = req.uri.path.toString
      val routeRegex = route.path
      val (viewNameAndActionStr, view_name, create_count_action) = routeRegex.unapplySeq(pathString).collect {
        case vna :: _ =>
          val CreateCountActionAndView(cca, vn) = vna
          if (viewDefs.contains(vn)) (vna, vn, cca)
          else (null, null, null)
      }.getOrElse((null, null, null))

      if (viewNameAndActionStr == null)
        WabaseRequestContext(route, viewDefs, req, null, null, null, null, null)
      else {
        val key = {
          def key_path(path: Path): Path = path match {
            case Segment(head, tail) =>
              if (head contains viewNameAndActionStr) tail
              else key_path(tail)
            case p => key_path(p.tail)
          }
          val keyPath = key_path(req.uri.path)
          def key(path: Path): List[String] = path match {
            case Segment(v, tail) => v :: key(tail)
            case Empty => Nil
            case p: SlashOrEmpty => key(p.tail)
          }
          key(keyPath)
        }

        val action = if (create_count_action != null) create_count_action else req.method match {
          case `GET`    =>
            if (key.nonEmpty || viewDefs.get(view_name)
              .exists(v => v.apiMethodToRoles.contains("get") && !v.apiMethodToRoles.contains("list")))
              Action.Get
            else
              Action.List
          case `POST`   => Action.Insert
          case `PUT`    => Action.Update
          case `DELETE` => Action.Delete
          case x        => error(s"Unsupported http method $x for request '${req.uri}'")
        }

        WabaseRequestContext(route, viewDefs, req, view_name, action, key, null, null)
      }
    }

    def invokeFunction(className: String, function: String, params: Seq[(Class[_], () => Any)]) = {
      val contextParams = Seq[(Class[_], () => Any)](
        (classOf[ExecutionContext], () => ec),
      )
      org.wabase.invokeFunction(className, function, params ++ contextParams)
    }

    def invokeReqTransChain(inv: Action.Invocation, wrc: WabaseRequestContext): Future[WabaseRequestContext] = {
      def invokeReqTrans(cn: String, fn: String, tctx: WabaseRequestContext): Future[WabaseRequestContext] = {
        def processResult(r: Any): Future[WabaseRequestContext] = r match {
          case ctx: WabaseRequestContext => Future.successful(ctx)
          case req: HttpRequest => processResult(ctx.copy(req = req))
          case f: Future[_] => f.flatMap(processResult)
          case x => error(s"Request transformer must return either WabaseRequestContext or HttpRequest or Future of them." +
            s" Instead got: $x")
        }

        processResult(invokeFunction(cn, fn, Seq((classOf[WabaseRequestContext], () => tctx))))
      }
      inv.arg match {
        case null => invokeReqTrans(inv.className, inv.function, wrc)
        case i: Action.Invocation => invokeReqTransChain(i, wrc)
          .flatMap(invokeReqTrans(inv.className, inv.function, _))
        case x => throw new IllegalArgumentException(s"Unrecognized request mapper argument $x, must be function call.")
      }
    }

    def invokeRespTransChain(
      inv: Action.Invocation,
      httpResp: HttpResponse,
      wrc: WabaseRequestContext
    ): Future[HttpResponse] = {
      def invokeRespTrans(cn: String, fn: String, resp: HttpResponse, tctx: WabaseRequestContext): Future[HttpResponse] = {
        def processResult(r: Any): Future[HttpResponse] = r match {
          case resp: HttpResponse => Future.successful(resp)
          case f: Future[_] => f.flatMap(processResult)
          case x => error(s"Response transformer must return either HttpResponse or Future of it." +
            s" Instead got: $x")
        }

        processResult(invokeFunction(cn, fn, Seq(
          (classOf[HttpResponse], () => resp),
          (classOf[WabaseRequestContext], () => tctx),
        )))
      }
      inv.arg match {
        case null => invokeRespTrans(inv.className, inv.function, httpResp, wrc)
        case i: Action.Invocation => invokeRespTransChain(i, httpResp, wrc)
          .flatMap(invokeRespTrans(inv.className, inv.function, _, wrc))
        case x => throw new IllegalArgumentException(s"Unrecognized response transformer argument $x, must be function call.")
      }
    }

    def doRequest(reqCtx: WabaseRequestContext): Future[HttpResponse] = {
      if (reqCtx.viewName == null)
        if (reqCtx.route.responseTransformer == null)
          error(s"If view name for route ${reqCtx.route.path} not specified, response transformer must be defined!")
        else invokeRespTransChain(reqCtx.route.responseTransformer, HttpResponse(), reqCtx)
      else {
        val httpResponseF = Future.successful(HttpResponse()) // TODO invoke do wabase action
        if (reqCtx.route.responseTransformer != null)
          httpResponseF.flatMap(invokeRespTransChain(reqCtx.route.responseTransformer, _, reqCtx))
        else httpResponseF
      }
    }

    Option(ctx.route.requestMapper)
      .map(invokeReqTransChain(_, ctx))
      .getOrElse(Future.successful(ctx)).flatMap { mappedCtx =>
        val ctxWithView = if (mappedCtx.viewName == null) viewActionKey(mappedCtx) else mappedCtx
        doRequest(ctxWithView)
    }
  }

  private def error(msg: String) = throw new WabaseRouteException(msg)
}

object WabaseService {
  def extractState(httpReqCtx: WabaseRequestContext)(app: WabaseApp[_]): Future[ApplicationState] = ???

  def optionalHttpHeaderValue[T](ctx: WabaseRequestContext, extractorF: HttpHeader => Option[T]): Option[T] = {
    ctx.req.headers.collectFirst(Function.unlift(extractorF))
  }

  def optionalHttpHeaderValueByName(ctx: WabaseRequestContext, name: String): Option[String] = {
    optionalHttpHeaderValue(ctx, optionalHttpHeaderValueExtractor(name.toLowerCase))
  }

  def optionalHttpHeaderValuePF[T](ctx: WabaseRequestContext, extractorPF: PartialFunction[HttpHeader, T]): Option[T] = {
    optionalHttpHeaderValue(ctx, extractorPF.lift)
  }

  def optionalHttpHeaderValueExtractor(lowerCaseName: String): HttpHeader => Option[String] = {
    case h: HttpHeader if h.is(lowerCaseName) => Some(h.value)
    case _                                    => None
  }

  def optionalCookie(ctx: WabaseRequestContext, name: String): Option[String] = {
    optionalHttpHeaderValue(ctx, {
      case Cookie(cookies) => cookies.find(_.name == name).map(_.value)
      case _               => None
    })
  }

  def setCookie(resp: HttpResponse, first: HttpCookie, more: HttpCookie*): HttpResponse = {
    resp.mapHeaders(_ ++ (first :: more.toList).map(`Set-Cookie`(_)))
  }

  def deleteCookie(resp: HttpResponse, name: String, domain: String = "", path: String = ""): HttpResponse = {
    val cookie = HttpCookie(name, "",
      domain = Option(domain).filter(_.nonEmpty), path = Option(path).filter(_.nonEmpty))
    resp.mapHeaders(_ ++ Seq(`Set-Cookie`(cookie.withValue("deleted").withExpires(DateTime.MinValue))))
  }
}
