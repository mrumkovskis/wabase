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
    YamlRawDefLoader.rawDefs("route", yamlMd, isRouteDef).transform { (routeName, rdMap) =>

      val parser = actionParser(routeName)

      def parseProperty(property: String) =
        parser(routeName)(rdMap).steps match {
          case Nil => null
          case List(Action.Evaluation(_, _, op: Action.Invocation)) => op
          case x => sys.error(s"Error parsing route $routeName $property, expected invocation call, got: $x")
        }

      val path: Regex = rdMap.get("path")
        .map(p => new Regex(p.toString))
        .getOrElse(sys.error(s"Path field not found for route $routeName"))
      val auth = parseProperty("auth")
      val state = parseProperty("state")
      val filter = parseProperty("req-filter")
      val transformer = parseProperty("resp-transformer")
      RouteDef(routeName, path, auth, state, filter, transformer)
    }.values.toList
  }

  private val routeDefPattern       = "(^|\\n)route\\s*:".r // XXX
  private def isRouteDef(d: YamlMd) = routeDefPattern.findFirstIn(d.body).isDefined
}
