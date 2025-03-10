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
        Option(inv).map(inv => List(inv.className, inv.function).filter(_ != null).mkString("."))
          .map(getObjectOrNewInstance(_, "wabase error handler").asInstanceOf[WabaseErrorHandler])
          .getOrElse(getObjectOrNewInstance[WabaseErrorHandler](config, "app.wabase-error-handler", "wabase error handler"))
      val path: Regex = new Regex(route)
      val mapper = parseProperty("request-mapper")
      val transformer = parseProperty("response-transformer")
      val error = errorHandler(parseProperty("error-handler"))
      RouteDef(
        path = path,
        requestMapper = mapper,
        responseTransformer = transformer,
        errorHandler = error,
      )
    }.values.toList
  }
}
