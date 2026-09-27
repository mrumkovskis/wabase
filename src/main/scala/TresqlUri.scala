package org.wabase

import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.http.scaladsl.model.Uri.{Path, Query}
import org.tresql.{Resources, RowLike, SingleValueResult, Query => TresqlQuery}
import org.tresql.ast.{Ast, BigDecimalConst, BinOp, Cast, Col, Cols, Exp, Filters, Ident, IntConst, Null, Obj, StringConst, TerOp, In, UnOp, Variable, Query => PQuery}
import org.tresql.parsing.QueryParsers

import java.net.URLEncoder
import scala.collection.immutable.{ListMap, Seq}

object TresqlUri {
  sealed trait TrUri
  case class Tresql(uriTresql: String) extends TrUri
  case class Uri(segments: Seq[Any], key: Seq[Any] = Nil, params: ListMap[String, String] = ListMap()) extends TrUri
}

trait TresqlUriParsers extends QueryParsers {

  /** Hyphenated word like `order-items` or `v2-beta-3` parsed as minus operation.
    * Matches only if all operands are idents or integers, so `:id-1` or `(a-b)` remain expressions. */
  private object Hyphenated {
    def unapply(e: Exp): Option[String] = e match {
      case BinOp("-", l, r) => for (a <- part(l); b <- part(r)) yield s"$a-$b"
      case _ => None
    }
    private def part(e: Exp): Option[String] = e match {
      case Ident(id) => Some(id.mkString("."))
      case Obj(id: Ident, null, null, null, _) => part(id)
      case IntConst(i) => Some(i.toString)
      case BigDecimalConst(d) if d.scale <= 0 => Some(d.toBigInt.toString)
      case b: BinOp => unapply(b)
      case _ => None
    }
  }

  /** convert idents and hyphenated words to string values */
  protected def uriComponentValue(value: Exp): Exp = value match {
    case Ident(id) => StringConst(id.mkString("."))
    case Obj(o, _, _, _, _) => uriComponentValue(o) // join and outer join marker ('?' before query) are not part of value
    case Hyphenated(s) => StringConst(s)
    case x => x
  }

  protected def queryParamName(exp: Exp): Option[String] = exp match {
    case Ident(id) => Some(id.mkString("."))
    case Obj(id: Ident, null, null, null, _) => queryParamName(id)
    case StringConst(v) => Some(v)
    case Hyphenated(s) => Some(s)
    case _ => None
  }

  /** quote alias if it is not a simple identifier, e.g. `page-size` */
  private def queryParamAlias(name: String): String =
    if (name.matches("""\p{L}[\p{L}\p{N}_]*""")) name else "\"" + name + "\""

  private def regroup(op: String, exp: Exp): Exp = {
    def rg(exp: Exp): Exp = exp match {
      case BinOp(o, l, r) if o == op => BinOp(o, rg(l),rg(r))
      case BinOp(o, l, r) =>
        val (nl, nr) = (rg(l), rg(r))
        nl match {
          case BinOp(ol, ll, rl) if ol == op => nr match {
            case BinOp(or, lr, rr) if or == op => BinOp(ol, ll, BinOp(or, rg(BinOp(o, rl, lr)), rr))
            case _ => BinOp(ol, ll, rg(BinOp(o, rl, nr)))
          }
          case _ => nr match {
            case BinOp(or, lr, rr) if or == op => BinOp(or, rg(BinOp(o, nl, lr)), rr)
            case _ => BinOp(o, nl, nr)
          }
        }
      case e => e
    }
    rg(exp)
  }

  private def splitBinOp(op: String, binOp: Exp): List[Exp] = {
    def split(exp: Exp): List[Exp] = exp match {
      case BinOp(o, l, r) if o == op => split(l) ::: split(r)
      case e => e :: Nil
    }
    split(regroup(op, binOp))
  }

  /** Raw path segment expressions, not converted by [[uriComponentValue]] so that
    * optional variable or outer join marker (trailing `?`) of the last segment can be detected */
  def pathSegments: MemParser[List[Exp]] = expr ^^ {
    case b: BinOp => splitBinOp("/", b)
    case e => List(e)
  } ^^ (_.flatMap {
    case PQuery(objs, Filters(Nil), null, null, null, null, null) if objs forall {
      case Obj(_, _, DefaultJoin, _, _) | Obj(_, _, null, _, _) => true
      case _ => false
    } => objs
    case x => List(x)
  }) named "uri-path-segments"

  def queryParameters: MemParser[List[Col]] = {
    def qp(p: Exp) = p match {
      case BinOp("=", name, value) if queryParamName(name).nonEmpty =>
        Col(uriComponentValue(value), queryParamAlias(queryParamName(name).get))
      case e => Col(uriComponentValue(e), Ast.toAlias(e))
    }
    expr into { e =>
      if (consumesFollowing(e)) err(UriNotEnclosedMsg) else success(splitBinOp("&", e).map(qp))
    }
  } named "uri-query-params"

  protected val UriNotEnclosedMsg =
    "Uri must be enclosed in parentheses when followed by other arguments, " +
      "e.g. http (a/b) headers = ..., http post (a/b) { :x x }"

  /** Uri expression has consumed following tokens */
  protected def consumesFollowing(e: Exp): Boolean = hasAlias(e) || isBareQuery(e)

  /** Alias outside subqueries means that uri has consumed following tokens, e.g. named argument name
    * in `a/b headers = ...` is parsed as alias of `b` */
  protected def hasAlias(e: Exp): Boolean = e match {
    case Col(_, a) if a != null => true
    case Obj(_, a, _, _, _) if a != null => true
    case Col(c, _) => hasAlias(c)
    case Obj(o, _, _, _, _) => hasAlias(o)
    case BinOp(_, l, r) => hasAlias(l) || hasAlias(r)
    case TerOp(l, _, m, _, r) => hasAlias(l) || hasAlias(m) || hasAlias(r)
    case In(l, r, _) => hasAlias(l) || r.exists(hasAlias)
    case UnOp(_, o) => hasAlias(o)
    case Cast(c, _) => hasAlias(c)
    case _ => false // subqueries, braces, functions may contain aliases
  }

  /** Query not enclosed in braces means that uri has consumed following tokens, e.g. body
    * in `a/b { :x x }` is parsed as columns of query `b`, filter argument in `a/b [:x]` as query filter */
  protected def isBareQuery(e: Exp): Boolean = e match {
    case _: PQuery => true
    case Col(c, _) => isBareQuery(c)
    case Obj(o, _, _, _, _) => isBareQuery(o)
    case BinOp(_, l, r) => isBareQuery(l) || isBareQuery(r)
    case TerOp(l, _, m, _, r) => isBareQuery(l) || isBareQuery(m) || isBareQuery(r)
    case In(l, r, _) => isBareQuery(l) || r.exists(isBareQuery)
    case UnOp(_, o) => isBareQuery(o)
    case Cast(c, _) => isBareQuery(c)
    case _ => false // subqueries in braces, functions
  }

  /** Leading slash of absolute path is prepended to the first segment. Separate empty string segment
    * is not used since some databases (Oracle) treat '' as null */
  private def absolutePath(segs: List[Exp]): List[Exp] = segs match {
    case StringConst(s) :: tail => StringConst("/" + s) :: tail
    case h :: tail => BinOp("||", StringConst("/"), h) :: tail
    case Nil => Nil
  }

  def uriParser: MemParser[PQuery] = opt("/") ~ pathSegments into {
    // column list syntax {'path', :id, '?', :v name}, possibly with from clause, is used as is
    case None ~ List(q: PQuery) if q.cols != null => success(q)
    case _ ~ segExps if segExps.exists(consumesFollowing) =>
      err(UriNotEnclosedMsg) // err - no backtracking, message must reach user
    case abs ~ segExps =>
      // trailing '?' may already be consumed by expr parser as optional variable or outer join marker
      val queryParPrefix: Parser[_] = segExps.last match {
        case v: Variable if v.opt => success(())
        case o: Obj if o.outerJoin == "l" => success(())
        case _ => "?"
      }
      val segVals = segExps.map(uriComponentValue)
      val segs = (if (abs.isDefined) absolutePath(segVals) else segVals).map(Col(_))
      opt(queryParPrefix ~> queryParameters) ^^
        (qp => segs ::: qp.map(p => Col(StringConst("?")) :: p).getOrElse(Nil)) ^^ { cols =>
        PQuery(List(Obj(Null, null, null, null)), Filters(Nil), Cols(cols), null, null, null, null)
      }
  } named "tresql-uri-parser"
}

class TresqlUri(
  /** When true, encode resource key in query string (?/key/parts); when false, in path.
    * Defaults to `app.key-in-query` config (true if unset). */
  val keyInQuery: Boolean =
    Option("app.key-in-query").filter(config.hasPath).forall(config.getBoolean)
) {
  private [wabase] def tresqlUriValue(trUri: TresqlUri.Tresql)(
    q: TresqlQuery, env: Map[String, Any], res: Resources): TresqlUri.Uri = {
    def uriValue(row: RowLike): TresqlUri.Uri = {
      val (names, vals) = (row match {
        case SingleValueResult(u: String) => Map((null, u))
        case SingleValueResult(u: Map[_, _]) => u
        case SingleValueResult(u: Iterable[_]) if u.size == 1 =>
          u.head match {
            case m: Map[_, _] => m
            case x => sys.error(s"Unable to retrieve uri value from [$x]")
          }
        case SingleValueResult(x) => sys.error(s"Unable to retrieve uri value from [$x]")
        case r => r.toMap
      }).toIndexedSeq.unzip
      val colCount = vals.size
      def sv(v: Any) = if (v == null) null else v.toString
      val (trUri, _) =
        (0 until colCount).foldLeft((TresqlUri.Uri(Nil), "s")) {
          case ((u, "s"), i) if sv(vals(i)) == "?/" => (u, "k")
          case ((u, "s"), i) if sv(vals(i)) == "?"  => (u, "p")
          case ((u, "k"), i) if sv(vals(i)) == "?"  => (u, "p")
          case ((u, "s"), i)  => (u.copy(segments = sv(vals(i)) :: u.segments.toList), "s")
          case ((u, "k"), i)  => (u.copy(key      = sv(vals(i)) :: u.key     .toList), "k")
          case ((u,  p ), i)  => (u.copy(params   = u.params + (names(i).toString -> sv(vals(i)))), "p")
        }
      trUri.copy(
        segments = trUri.segments.reverse,
        key      = trUri.key.reverse
      )
    }
    uriValue(q(trUri.uriTresql, env)(res).unique)
  }

  // pekko http uri methods
  def keyToUriStrings(key: Seq[Any]): Seq[String] = key.map {
    case t: java.time.temporal.Temporal => Format.convertToString(t).replace(' ', '_').replace('T', '_')
    case t: java.util.Date              => Format.convertToString(t).replace(' ', '_').replace('T', '_')
    case x => s"$x"
  }

  def uriWithKeyInPath(uri: Uri, key: Seq[Any]): Uri =
    if (key != null && key.nonEmpty)
      uri.withPath(keyToUriStrings(key).foldLeft(uri.path) { (p, k) => p / s"$k" })
    else uri

  def uriWithKeyInQuery(uri: Uri, key: Seq[Any]): Uri = {
    def encode(s: String) =
      URLEncoder.encode(s"$s", "UTF-8")
        .replace("+", "%20")
        .replace("%3A", ":") // allowed, do not be ugly with timestamps

    if (key != null && key.nonEmpty) {
      val keyPathRawQuery = keyToUriStrings(key).map(encode).mkString("/", "/", "")
      uri.withRawQueryString(
        uri.rawQueryString.map(q => s"$keyPathRawQuery?$q") getOrElse keyPathRawQuery)
    } else uri
  }

  /** Key representation in redirect / Location uri.
    * Uses [[uriWithKeyInQuery]] when [[keyInQuery]] is true (default, `app.key-in-query`),
    * otherwise [[uriWithKeyInPath]]. Override or construct with `keyInQuery = false` to change.
    */
  def uriWithKey(uri: Uri, key: Seq[Any]): Uri =
    if (keyInQuery) uriWithKeyInQuery(uri, key) else uriWithKeyInPath(uri, key)

  def fromTresqlUri(value: TresqlUri.Tresql)(q: TresqlQuery, env: Map[String, Any], res: Resources): Uri =
    uri(tresqlUriValue(value)(q, env, res))

  def uri(value: TresqlUri.Uri): Uri = {
    require(value.segments != null && value.segments.nonEmpty, "Uri segments must not be empty!")
    val uriRegex = """(?U)(https?://[^/]+)?(?:(?:$)|(.+))?""".r
    val uriRegex(uriStart, uriPath) = value.segments.mkString("/"): @unchecked
    val path = Option(uriPath).map(Path(_)).getOrElse(Path.Empty)
    val nonNullParams = value.params.map { case (k, v) => (k, if (v == null) "" else v) }
    val uriWithoutKey =
      Option(uriStart).map(Uri(_)).getOrElse(Uri.Empty)
        .withPath(path)
        .withQuery(Query(nonNullParams))
    uriWithKey(uriWithoutKey, value.key.toVector)
  }
}
