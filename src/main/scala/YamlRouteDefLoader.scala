package org.wabase

import org.apache.pekko.http.scaladsl.model.{HttpMethod, HttpMethods}
import org.mojoz.metadata.in.YamlMd
import org.wabase.AppMetadata.{Action, PathNameAndParameters, PathParameter, RouteDef}

import scala.collection.immutable._
import scala.util.matching.Regex
import scala.util.parsing.combinator.Parsers
import scala.util.parsing.input.CharSequenceReader


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

  protected def regexAndPathNamesAndParameters(pattern: String, rdMap: Map[String, Any]): (Regex, Seq[PathNameAndParameters]) =
    try new Regex(pattern) -> RegexPath.pathNamesAndParameters(pattern) catch {
      case util.control.NonFatal(ex) =>
        if (SwaggerPath.isValid(pattern))
          (new Regex(SwaggerPath.toRegexString(pattern)),
           Seq(PathNameAndParameters(
            pattern,
            SwaggerPath.extractPathParameters(pattern).map { parameterName =>
              PathParameter(parameterName, "string", null)
            }
           ))
          )
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

      def parseProperty(property: String, pathParams: Seq[String]) =
        parser(property)(rdMap).steps match {
          case Nil => null
          case List((Action.Evaluation(_, _, op: Action.Invocation), _)) =>
            //transform named params to regex group index params
            val NamedParamRegex = """\$([^\d][^\s]*)""".r
            def transform(o: Action.Op): Action.Op = o match {
              case t: Action.Tresql if NamedParamRegex.pattern.matcher(t.tresql).matches() =>
                val NamedParamRegex(paramName) = t.tresql
                val idx = pathParams.indexOf(paramName)
                if (idx == -1) sys.error(s"Error parsing route $route $property: unknown parameter name $paramName")
                else t.copy(tresql = "$" + (idx + 1))
              case i: Action.Invocation => i.copy(args = i.args map transform)
              case x => x
            }
            transform(op).asInstanceOf[Action.Invocation]
          case x => sys.error(s"Error parsing route $route $property, expected invocation call, got: $x")
        }

      def errorHandler(inv: AppMetadata.Action.Invocation) =
        Option(inv).getOrElse {
          val (cn, fn) = classNameFunctionName(config.getString("app.wabase-error-handler"))
          AppMetadata.Action.Invocation(cn, fn)
        }
      val PathRegex(m, p) = route
      val method = if (m.trim.isEmpty) Set[HttpMethod]() else m.split("\\s+").map(httpMethods(_)).toSet
      val (path, pathNamesAndParameters): (Regex, Seq[PathNameAndParameters]) = regexAndPathNamesAndParameters(p, rdMap)
      val pathParameterNames =
        if  (pathNamesAndParameters.size == 1)
             pathNamesAndParameters.head.parameters.map(_.name)
        else Nil
      val handler = Option(parseProperty("do", pathParameterNames)).getOrElse(sys.error(s"Request handler missing"))
      val error = errorHandler(parseProperty("recover", pathParameterNames))
      val extras = rdMap - "on" - "do" - "recover"
      RouteDef(
        methods = method,
        path = path,
        requestHandler = handler,
        errorHandler = error,
        pathNamesAndParameters = pathNamesAndParameters,
        extras = extras,
      )
    }.toList
  }
}

object RegexPath {
  sealed trait RegexAST
  case class Literal(s: String) extends RegexAST
  case class Dot() extends RegexAST
  case class CharClass(s: String) extends RegexAST
  case class Escaped(c: Char) extends RegexAST
  case class Group(inner: RegexAST) extends RegexAST
  case class NamedGroup(name: String, inner: RegexAST) extends RegexAST
  case class NonCapturingGroup(inner: RegexAST) extends RegexAST
  case class Alternation(branches: Seq[RegexAST]) extends RegexAST
  case class Concat(parts: Seq[RegexAST]) extends RegexAST
  case class Quantified(item: RegexAST, q: String) extends RegexAST

  class RegexParser extends Parsers {
    type Elem = Char

    def noneOf(cs: String): Parser[Char] = elem("noneOf", c => !cs.contains(c))

    def anyChar: Parser[Char] = elem("any", _ => true)

    def literal(c: Char): Parser[Char] = accept(c)

    def rep1sep[A](p: Parser[A], sep: Parser[Any]): Parser[Seq[A]] = p ~ rep(sep ~> p) ^^ { case x ~ xs => x :: xs }

    def letterOrDigit: Parser[Char] = elem("alphanum", c => Character.isLetterOrDigit(c))

    def id: Parser[String] = (letterOrDigit | accept('_')) ~ rep(letterOrDigit | accept('_')) ^^ { case first ~ rest => s"$first${rest.mkString}" }

    def regex: Parser[RegexAST] = alternation

    def alternation: Parser[RegexAST] = rep1sep(branch, literal('|')) ^^ {
      case list if list.length == 1 => list.head
      case list => Alternation(list)
    }

    def branch: Parser[RegexAST] = rep1(piece) ^^ {
      case list if list.length == 1 => list.head
      case list => Concat(list)
    }

    def piece: Parser[RegexAST] = atom ~ opt(quantifier) ^^ {
      case a ~ Some(q) => Quantified(a, q)
      case a ~ None => a
    }

    def atom: Parser[RegexAST] =
      literal('(') ~> groupType <~ literal(')') |
        literal('.') ^^^ Dot() |
        literal('[') ~> classContent <~ literal(']') ^^ CharClass |
        literal('\\') ~> anyChar ^^ Escaped |
        noneOf(".^$*+?()|[{\\") ^^ { c => Literal(c.toString) }

    def groupType: Parser[RegexAST] =
      (literal('?') ~> literal('<') ~> id <~ literal('>')) ~ regex ^^ { case name ~ inner => NamedGroup(name, inner) } |
        (literal('?') ~> literal(':')) ~ regex ^^ { case _ ~ inner => NonCapturingGroup(inner) } |
        regex ^^ Group

    def quantifier: Parser[String] =
      literal('*') ^^^ "*" |
        literal('+') ^^^ "+" |
        literal('?') ^^^ "?" |
        literal('{') ~> rep1(anyChar) <~ literal('}') ^^ { chars => "{" + chars.mkString + "}" }

    def classContent: Parser[String] = rep(classChar) ^^ (_.mkString)

    def classChar: Parser[String] =
      (literal('\\') ~> anyChar ^^ { c => "\\" + c.toString }) |
        noneOf("]") ^^ (_.toString)
  }

  private def mergeLiterals(ast: RegexAST): RegexAST = ast match {
    case Concat(parts) =>
      val mergedParts = parts.map(mergeLiterals).foldLeft(Seq.empty[RegexAST]) { (acc, curr) =>
        (acc.lastOption, curr) match {
          case (Some(Literal(s1)), Literal(s2)) => acc.init :+ Literal(s1 + s2)
          case _ => acc :+ curr
        }
      }
      if (mergedParts.length == 1) mergedParts.head else Concat(mergedParts)
    case Group(inner) => Group(mergeLiterals(inner))
    case NamedGroup(name, inner) => NamedGroup(name, mergeLiterals(inner))
    case NonCapturingGroup(inner) => NonCapturingGroup(mergeLiterals(inner))
    case Alternation(branches) => Alternation(branches.map(mergeLiterals))
    case Quantified(item, q) => Quantified(mergeLiterals(item), q)
    case other => other
  }

  private def getLiteral(ast: RegexAST): Option[String] = ast match {
    case Literal(s) => Some(s)
    case Concat(parts) =>
      val lits = parts.map(getLiteral)
      if (lits.forall(_.isDefined)) Some(lits.map(_.get).mkString) else None
    case Group(inner) => getLiteral(inner)
    case NamedGroup(_, inner) => getLiteral(inner)
    case NonCapturingGroup(inner) => getLiteral(inner)
    case Escaped(c) => if ("\\/-_. *+?()[]{}|^$".contains(c)) Some(c.toString) else None
    case _ => None
  }

  private def toRegex(ast: RegexAST): String = ast match {
    case Literal(s) => s.replaceAll("([.^$*+?(){}\\[\\|\\-])", "\\\\$1")
    case Dot() => "."
    case CharClass(s) => "[" + s + "]"
    case Escaped(c) => "\\" + c
    case Group(inner) => "(" + toRegex(inner) + ")"
    case NamedGroup(name, inner) => "(" + toRegex(inner) + ")"
    case NonCapturingGroup(inner) => "(?:" + toRegex(inner) + ")"
    case Alternation(branches) => branches.map { b =>
      val str = toRegex(b)
      if (b.isInstanceOf[Alternation] || b.isInstanceOf[Concat] && branches.size > 1) "(" + str + ")" else str
    }.mkString("|")
    case Concat(parts) => parts.map { p =>
      val str = toRegex(p)
      if (p.isInstanceOf[Alternation]) "(" + str + ")" else str
    }.mkString
    case Quantified(item, q) =>
      val str = toRegex(item)
      val itemStr = if (item.isInstanceOf[Alternation] || item.isInstanceOf[Concat]) "(" + str + ")" else str
      itemStr + q
  }

  private def extractLeadingLiteral(ast: RegexAST): (String, RegexAST) = ast match {
    case Literal(s) => (s, Literal(""))
    case Concat(parts) =>
      parts match {
        case Nil => ("", Concat(Nil))
        case head :: tail =>
          val (pre, remHead) = extractLeadingLiteral(head)
          if (pre.nonEmpty) (pre, if (remHead == Literal("")) Concat(tail) else Concat(remHead :: tail))
          else ("", ast)
      }
    case Group(inner) =>
      val (pre, rem) = extractLeadingLiteral(inner)
      (pre, Group(rem))
    case NamedGroup(name, inner) =>
      val (pre, rem) = extractLeadingLiteral(inner)
      (pre, NamedGroup(name, rem))
    case NonCapturingGroup(inner) =>
      val (pre, rem) = extractLeadingLiteral(inner)
      (pre, NonCapturingGroup(rem))
    case _ => ("", ast)
  }

  type Result = (String, Seq[(String, String)], Int)

  private def handleDynamic(ast: RegexAST, count: Int, nameOpt: Option[String] = None): Set[Result] = {
    val (pre, rem) = extractLeadingLiteral(ast)
    val pName = nameOpt.getOrElse(s"p$count")
    val nextCount = if (nameOpt.isDefined) count else count + 1
    getLiteral(rem) match {
      case Some(suf) => Set((pre + suf, Seq(), nextCount))
      case None =>
        val remRegex = if (rem == Literal("") || rem == Concat(Seq())) "" else toRegex(rem)
        val paramRegex = if (remRegex.isEmpty) "" else "^" + remRegex + "$"
        if (paramRegex.isEmpty) {
          Set((pre, Seq(), nextCount))
        } else {
          Set((pre + s"{$pName}", Seq((pName, paramRegex)), nextCount))
        }
    }
  }

  private def rec(ast: RegexAST, count: Int): Set[Result] = ast match {
    case Literal(s) => Set((s, Seq(), count))
    case Escaped(c) =>
      getLiteral(Escaped(c)) match {
        case Some(s) => Set((s, Seq(), count))
        case None => handleDynamic(Escaped(c), count)
      }
    case Dot() => handleDynamic(Dot(), count)
    case CharClass(s) => handleDynamic(CharClass(s), count)
    case Group(inner) =>
      getLiteral(inner) match {
        case Some(s) => Set((s, Seq(), count))
        case None => handleDynamic(inner, count)
      }
    case NamedGroup(name, inner) =>
      getLiteral(inner) match {
        case Some(s) => Set((s, Seq(), count))
        case None => handleDynamic(inner, count, Some(name))
      }
    case NonCapturingGroup(inner) => rec(inner, count)
    case Concat(parts) =>
      parts.foldLeft(Set[Result](("", Seq(), count))) { (acc, part) =>
        acc.flatMap { case (path, params, nextCount) =>
          rec(part, nextCount).map { case (subPath, subParams, newNext) =>
            (path + subPath, params ++ subParams, newNext)
          }
        }
      }
    case Alternation(branches) => handleDynamic(ast, count)
    case Quantified(item, q) =>
      if (q == "?") {
        val (pre, rem) = extractLeadingLiteral(item)
        if (pre == "/") {
          val without = Set(("", Seq(), count))
          val withSets = rec(rem, count).map { case (p, pr, nc) => ("/" + p, pr, nc) }
          without ++ withSets
        } else {
          handleDynamic(ast, count)
        }
      } else {
        handleDynamic(ast, count)
      }
  }

  private def process(ast: RegexAST): Set[(String, Seq[(String, String)])] = {
    rec(ast, 1).map { case (path, params, _) => (path, params) }
  }

  def convert(regex: String): Seq[(String, Seq[(String, String)])] = {
    var cleaned = regex
    if (cleaned.startsWith("^")) cleaned = cleaned.substring(1)
    if (cleaned.endsWith("$")) cleaned = cleaned.dropRight(1)
    val parser = new RegexParser
    val reader = new CharSequenceReader(cleaned)
    parser.regex(reader) match {
      case parser.Success(rawAst, _) =>
        val ast = mergeLiterals(rawAst)
        val branches = ast match {
          case Alternation(bs) => bs
          case other => Seq(other)
        }
        branches.flatMap(process).toList.sortBy(_._1)
      case other => throw new IllegalArgumentException(s"Failed to parse regex: $other")
    }
  }

  def pathNamesAndParameters(regex: String): Seq[PathNameAndParameters] = {
    try {
      convert(regex).map {
        case (name, params) =>
          PathNameAndParameters(
            name,
            params.map { case (name, pattern) => PathParameter(name, "string", pattern) }
          )
      }
    } catch {
      case util.control.NonFatal(ex) =>
        Seq(PathNameAndParameters(regex, Nil)) // not supported - return as is
    }
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
