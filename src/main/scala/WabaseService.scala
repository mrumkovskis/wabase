package org.wabase

import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model.Uri.Path.{Empty, Segment, SlashOrEmpty}
import akka.http.scaladsl.server.RequestContext
import org.wabase.AppMetadata.RouteDef

case class WabaseRequestContext(ctx: RequestContext, viewName: String, action: String, key: Seq[Any])

class WabaseRouteNotFoundException(message: String) extends Exception(message)

object WabaseService {

  private val ActionAndView = """(?U)(?:(count|create):)?(\w*)""".r

  def requestContext(routes: Seq[RouteDef])(ctx: RequestContext): WabaseRequestContext = {
    val pathString = ctx.request.uri.path.toString
    val route = routes.find(_.path.pattern.matcher(pathString).matches)
      .getOrElse(error(s"Route not found for path '$pathString'"))
    val routeRegex = route.path
    val routeRegex(viewNameAndActionStr) = pathString
    // view name may contain count or create action
    def view_action_key(path: Path): (String, Path) = path match {
      case Segment(head, tail) =>
        if (head contains viewNameAndActionStr) (head, tail)
        else view_action_key(tail)
      case p => view_action_key(p)
    }
    val (view_action, keyPath) = view_action_key(ctx.request.uri.path)
    val ActionAndView(action, viewName) = view_action
    def key(path: Path): List[String] = path match {
      case Segment(v, tail) => v :: key(tail)
      case Empty => Nil
      case p: SlashOrEmpty => key(p.tail)
    }
    WabaseRequestContext(ctx, viewName, action, key(keyPath))
  }

  private def error(msg: String) = throw new WabaseRouteNotFoundException(msg)
}
