package org.wabase

import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model.Uri.Path.{Empty, Segment, SlashOrEmpty}
import akka.http.scaladsl.server.{LanguageNegotiator, RequestContext}
import org.mojoz.metadata.ViewDef
import AppMetadata._
import akka.http.scaladsl.model.headers.Cookie
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import org.wabase.AppMetadata.{Action, RouteDef}

import java.util.Locale
import scala.concurrent.{ExecutionContext, Future}

case class WabaseUser(properties: Map[String, Any]) {
  val id: Long      = properties.get("id").collect { case x: Number => x.longValue }.getOrElse(-1)
  val name: String  = properties.get("name").map(String.valueOf).orNull
}

case class WabaseRequestContext(
  route: RouteDef,
  req: HttpRequest,
  viewName: String,
  action: String,
  key: Seq[Any],
  applicationState: ApplicationState,
  user: WabaseUser,
)

class WabaseRouteException(message: String) extends Exception(message)
/** Can be used for example in authentication filter to return HTTP Unauthorized */
class WabaseRouteFilterException(response: HttpResponse) extends Exception

class WabaseService {

  private val CreateCountActionAndView = """(?U)(?:(count|create):)?(\w*)""".r

  def requestContext(routes: Seq[RouteDef], viewDefs: Map[String, ViewDef])(req: HttpRequest): WabaseRequestContext = {
    val pathString = req.uri.path.toString
    val route = routes.find(_.path.pattern.matcher(pathString).matches)
      .getOrElse(error(s"Route not found for path '$pathString'"))
    val routeRegex = route.path
    val (viewNameAndActionStr, view_name, create_count_action) = routeRegex.unapplySeq(pathString).collect {
      case vna :: _ =>
        val CreateCountActionAndView(cca, vn) = vna
        if (viewDefs.contains(vn)) (vna, vn, cca)
        else (null, null, null)
    }.getOrElse((null, null, null))

    if (viewNameAndActionStr == null)
      WabaseRequestContext(route, req, null, null, null, null, null)
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

      WabaseRequestContext(route, req, view_name, action, key, null, null)
    }
  }

  def doRequest(ctx: WabaseRequestContext)(implicit ec: ExecutionContext): Future[HttpResponse] = {
    import ctx._
    if (viewName == null) {
      def invokeFunction(className: String, function: String, params: Seq[(Class[_], Class[_] => Any)]) = {
        val contextParams = Seq[(Class[_], Class[_] => Any)](
          (classOf[ExecutionContext], _ => ec),
        )
        org.wabase.invokeFunction(className, function, params ++ contextParams)
      }
      def invokeReqTrans(cn: String, fn: String): Future[WabaseRequestContext] = {
        def processResult(r: Any): Future[WabaseRequestContext] = r match {
          case ctx: WabaseRequestContext => Future.successful(ctx)
          case req: HttpRequest => processResult(ctx.copy(req = req))
          case f: Future[_] => f.flatMap(processResult)
          case x => error(s"Request transformer must return either WabaseRequestContext or HttpRequest or Future of them." +
            s" Instead got: $x")
        }
        processResult(invokeFunction(cn, fn, Seq((classOf[WabaseRequestContext], _ => ctx))))
      }
      def invokeRespTrans(cn: String, fn: String, tctx: WabaseRequestContext): Future[HttpResponse] = {
        def processResult(r: Any): Future[HttpResponse] = r match {
          case resp: HttpResponse => Future.successful(resp)
          case f: Future[_] => f.flatMap(processResult)
          case x => error(s"Response transformer must return either HttpResponse or Future of it." +
            s" Instead got: $x")
        }
        processResult(invokeFunction(cn, fn, Seq((classOf[WabaseRequestContext], _ => tctx))))
      }
      if (route.responseTransformer == null) error(s"If view name for route not specified, response transformer must be defined!")
      else Option(route.requestFilter)
        .map { case Action.Invocation(cn, fn, _, _) =>
          invokeReqTrans(cn, fn)
        }.getOrElse(Future.successful(ctx))
        .flatMap { tctx =>
          import route.responseTransformer._
          invokeRespTrans(className, function, tctx)
        }
    } else ???
  }

  private def error(msg: String) = throw new WabaseRouteException(msg)

  protected def extractState: WabaseRequestContext => WabaseApp[_] => Future[ApplicationState] =
    WabaseService.extractState
}

object WabaseService {
  def extractState(reqCtx: WabaseRequestContext)(app: WabaseApp[_]): Future[ApplicationState] = ???
}
