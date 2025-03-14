package org.wabase

import org.mojoz.metadata.in.YamlMd
import org.wabase.AppMetadata.{Action, RouteDef}

import scala.collection.immutable._
import scala.util.matching.Regex

class YamlRouteDefLoader(
  yamlMd: Seq[YamlMd],
  actionParser: String => String => Map[String, Any] => Action,
) {

  lazy val routeDefs: Seq[RouteDef] = {
    val ds = yamlMd.flatMap(_.parsed).filter(_ contains "path").map(s => s("path").toString -> s)
    ds.toMap.transform { (route, rdMap) =>

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
      val path: Regex = new Regex(route)
      val handler = Option(parseProperty("handler")).getOrElse(sys.error(s"Request handler missing"))
      val error = errorHandler(parseProperty("error-handler"))
      RouteDef(
        path = path,
        requestHandler = handler,
        errorHandler = error,
      )
    }.values.toList
  }
}
