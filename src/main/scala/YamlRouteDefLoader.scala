package org.wabase

import org.apache.pekko.http.scaladsl.model.HttpMethods
import org.mojoz.metadata.in.YamlMd
import org.wabase.AppMetadata.{Action, RouteDef}

import scala.collection.immutable._
import scala.util.matching.Regex

class YamlRouteDefLoader(
  yamlMd: Seq[YamlMd],
  actionParser: String => String => Map[String, Any] => Action,
) {


  private val httpMethods = Map(
    HttpMethods.CONNECT.value   -> HttpMethods.CONNECT,
    HttpMethods.DELETE.value    -> HttpMethods.DELETE,
    HttpMethods.GET.value       -> HttpMethods.GET,
    HttpMethods.HEAD.value      -> HttpMethods.HEAD,
    HttpMethods.OPTIONS.value   -> HttpMethods.OPTIONS,
    HttpMethods.PATCH.value     -> HttpMethods.PATCH,
    HttpMethods.POST.value      -> HttpMethods.POST,
    HttpMethods.PUT.value       -> HttpMethods.PUT,
    HttpMethods.TRACE.value     -> HttpMethods.TRACE,
  )

  private val PathRegex = new Regex(s"((${httpMethods.keys.mkString("|")})\\s+)?(.+)")

  lazy val routeDefs: Seq[RouteDef] = {
    val ds = yamlMd.flatMap(_.parsed).filter(_ contains "on").map(s => s("on").toString -> s)
    ds.map { case (route, rdMap) =>

      val parser = actionParser(route)

      def parseProperty(property: String) =
        parser(property)(rdMap).steps match {
          case Nil => null
          case List(Action.Evaluation(_, _, op: Action.Invocation, _)) => op
          case x => sys.error(s"Error parsing route $route $property, expected invocation call, got: $x")
        }

      def errorHandler(inv: AppMetadata.Action.Invocation) =
        Option(inv).getOrElse {
          val (cn, fn) = OpParser.classNameFunctionName(config.getString("app.wabase-error-handler"))
          AppMetadata.Action.Invocation(cn, fn)
        }
      val PathRegex(_, m, p) = route
      val method = httpMethods.getOrElse(m, null)
      val path: Regex = new Regex(p)
      val handler = Option(parseProperty("do")).getOrElse(sys.error(s"Request handler missing"))
      val error = errorHandler(parseProperty("recover"))
      RouteDef(
        method = method,
        path = path,
        requestHandler = handler,
        errorHandler = error,
      )
    }.toList
  }
}
