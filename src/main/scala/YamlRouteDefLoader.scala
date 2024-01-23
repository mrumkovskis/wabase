package org.wabase

import org.mojoz.metadata.in.YamlMd
import org.wabase.AppMetadata.{Action, RouteDef}

import scala.collection.immutable._
import scala.util.matching.Regex

class YamlRouteDefLoader(
  yamlMd: Seq[YamlMd],
  requestTransfomerParser:    String => Map[String, Any] => Action,
  responseTransformerParser:  String => Map[String, Any] => Action,
) {

  lazy val routeDefs: Seq[RouteDef] = {
    YamlRawDefLoader.rawDefs("route", yamlMd, isRouteDef).transform { (routeName, rdMap) =>

      def parseTransformer(trName: String, parser: String => Map[String, Any] => Action) =
        parser(routeName)(rdMap).steps match {
          case Nil => null
          case List(Action.Evaluation(_, _, op: Action.Invocation)) => op
          case x => sys.error(s"Error parsing route $routeName $trName, expected invocation call, got: $x")
        }

      val path: Regex = rdMap.get("path")
        .map(p => new Regex(p.toString))
        .getOrElse(sys.error(s"Path field not found for route $routeName"))
      val requestTransformer = parseTransformer("request transformer", requestTransfomerParser)
      val responseTransformer = parseTransformer("response transformer", responseTransformerParser)
      RouteDef(routeName, path, requestTransformer, responseTransformer)
    }.values.toList
  }

  private val routeDefPattern       = "(^|\\n)route\\s*:".r // XXX
  private def isRouteDef(d: YamlMd) = routeDefPattern.findFirstIn(d.body).isDefined
}
