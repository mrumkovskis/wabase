package org.wabase

import org.apache.pekko.http.scaladsl.model.{HttpMethod, HttpMethods}
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

  private val PathRegex = new Regex(s"((?:(?:${httpMethods.keys.mkString("|")})\\s+)*)?(.+)")

  protected def regexAndPathParameterNames(pattern: String, rdMap: Map[String, Any]): (Regex, Seq[String]) =
    try new Regex(pattern) -> Nil catch {
      case util.control.NonFatal(ex) =>
        if (SwaggerPath.isValid(pattern))
          (new Regex(SwaggerPath.toRegexString(pattern)), SwaggerPath.extractPathParameters(pattern))
        else throw new RuntimeException(
          s"Pattern '$pattern' failed to compile as regex and is not a valid swagger path pattern", ex)
    }

  lazy val routeDefs: Seq[RouteDef] = {
    val ds = yamlMd.flatMap(_.parsed)
      .filter(_ contains "on")
      .map(s => MapUtils.javaMapToMap(MapUtils.mapToJavaMap(s)))
      .map(s => s("on").toString -> s)
    ds.map { case (route, rdMap) =>

      val parser = actionParser(route)

      def parseProperty(property: String) =
        parser(property)(rdMap).steps match {
          case Nil => null
          case List((Action.Evaluation(_, _, op: Action.Invocation, _), _)) => op
          case x => sys.error(s"Error parsing route $route $property, expected invocation call, got: $x")
        }

      def errorHandler(inv: AppMetadata.Action.Invocation) =
        Option(inv).getOrElse {
          val (cn, fn) = OpParser.classNameFunctionName(config.getString("app.wabase-error-handler"))
          AppMetadata.Action.Invocation(cn, fn)
        }
      val PathRegex(m, p) = route
      val method = if (m.trim.isEmpty) Set[HttpMethod]() else m.split("\\s+").map(httpMethods(_)).toSet
      val (path, pathParameterNames): (Regex, Seq[String]) = regexAndPathParameterNames(p, rdMap)
      val handler = Option(parseProperty("do")).getOrElse(sys.error(s"Request handler missing"))
      val error = errorHandler(parseProperty("recover"))
      val extras = rdMap - "on" - "do" - "recover"
      RouteDef(
        methods = method,
        path = path,
        requestHandler = handler,
        errorHandler = error,
        pathParameterNames = pathParameterNames,
        extras = extras,
      )
    }.toList
  }
}

object SwaggerPath {
  private val charClass                     =  """[^{}/\s]"""
  private val requiredChars                 = s"""$charClass+"""
  private val optionalChars                 = s"""$charClass*"""
  private val paramPattern                  = s"""\\{$requiredChars\\}"""
  private val paramWithOptionalStatic       = s"""$paramPattern$optionalChars"""
  private val segmentOptionStartsWithStatic = s"""$requiredChars($paramWithOptionalStatic)*"""
  private val segmentOptionStartsWithParam  = s"""$optionalChars($paramWithOptionalStatic)+"""
  private val segmentPattern                = s"""($segmentOptionStartsWithStatic|$segmentOptionStartsWithParam)"""
  private val repeatingSegmentsWithSlash    = s"""($segmentPattern/)*"""
  private val optionalFinalSegment          = s"""($segmentPattern)?"""
  private val pathRegexStr                  = s"""^/$repeatingSegmentsWithSlash$optionalFinalSegment$$"""
  private val pathRegex                     = pathRegexStr.r
  private val paramRegex                    = """\{([^}]+)\}""".r

  def isValid(path: String): Boolean = {
    pathRegex.pattern.matcher(path).matches
  }

  def extractPathParameters(path: String): Seq[String] = {
    paramRegex.findAllMatchIn(path).map(_.group(1)).toVector
  }

  def toRegexString(path: String, paramExtractors: Seq[String]): String = {
    val sb = new StringBuilder
    var i = 0
    var extractorsSeq = paramExtractors
    while (i < path.length) {
      if (path.charAt(i) == '{') {
        // Skip to the closing brace
        var j = i + 1
        while (j < path.length && path.charAt(j) != '}') j += 1
        // Assuming valid path, no need for unbalanced check
        sb.append(extractorsSeq.head)
        extractorsSeq = extractorsSeq.tail
        i = j + 1
      } else {
        // Append static character, escaping regex specials
        val ch = path.charAt(i)
        if ("^$\\.*+?()[]{}|".contains(ch)) {
          sb.append('\\').append(ch)
        } else {
          sb.append(ch)
        }
        i += 1
      }
    }
    sb.toString
  }

  def toRegexString(path: String, paramExtractor: String = "([^/]+)"): String =
    toRegexString(path, extractPathParameters(path).map(_ => paramExtractor))
}
