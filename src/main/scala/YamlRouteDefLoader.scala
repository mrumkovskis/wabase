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
        parser(route)(rdMap).steps match {
          case Nil => null
          case List(Action.Evaluation(_, _, op: Action.Invocation)) => op
          case x => sys.error(s"Error parsing route $route $property, expected invocation call, got: $x")
        }

      val path: Regex = new Regex(route)
      val filter = parseProperty("request-filter")
      val transformer = parseProperty("response-transformer")
      val state = parseProperty("state")
      val auth = parseProperty("authentication")
      val authz = parseProperty("authorization")
      val processor = parseProperty("processor")
      val session = parseProperty("session")
      val error = parseProperty("error")
      RouteDef(
        path = path,
        requestFilter = filter,
        responseTransformer = transformer,
        state = state,
        authentication = auth,
        authorization = authz,
        processor = processor,
        session = session,
        error = error,
      )
    }.values.toList
  }
}
