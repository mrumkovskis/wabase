package org.wabase

import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.http.scaladsl.model.Uri.{Path, Query}
import org.tresql.{Resources, RowLike, SingleValueResult, Query => TresqlQuery}
import org.tresql.ast.{Col, Cols, Const, Exp, Null, Obj, StringConst, Variable, Query => PQuery}
import org.tresql.parsing.QueryParsers

import java.net.URLEncoder
import scala.collection.immutable.{ListMap, Seq}

object TresqlUri {
  sealed trait TrUri
  case class Tresql(uriTresql: String) extends TrUri
  case class Uri(segments: Seq[Any], key: Seq[Any] = Nil, params: ListMap[String, String] = ListMap()) extends TrUri
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
