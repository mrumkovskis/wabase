package org.wabase

import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.Uri.{Path, Query}
import org.tresql.{Resources, RowLike, SingleValueResult, Query => TresqlQuery}
import org.tresql.ast.{Col, Cols, Const, Exp, Null, Obj, StringConst, Variable, Query => PQuery}
import org.tresql.parsing.QueryParsers

import java.net.URLEncoder
import scala.collection.immutable.{ListMap, Seq}

object TresqlUri {
  sealed trait TrUri
  case class Tresql(uriTresql: String) extends TrUri
  case class Uri(
    value: String,
    key: Seq[Any] = Nil,
    params: ListMap[String, String] = ListMap(),
    keyInPath: Boolean = false,
  )
}

class TresqlUri {
  def tresqlUriValue(trUri: TresqlUri.TrUri, keyInPath: Boolean)(
    q: TresqlQuery, env: Map[String, Any], res: Resources): TresqlUri.Uri = trUri match {
    case TresqlUri.Tresql(t) => uriValue(q(t, env)(res).unique, 0, keyInPath)
  }

  def uriValue(row: RowLike, startIdx: Int, keyInPath: Boolean): TresqlUri.Uri = {
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
    val (value, (key, params, _)) = (sv(vals(startIdx)),
      ((startIdx + 1) until colCount).foldLeft((List[String](), ListMap[String, String](), false)) {
        case ((k, p, _), i) if sv(vals(i)) == "?" => (k, p, true)
        case ((k, p, false), i) => (sv(vals(i)) :: k, p, false)
        case ((k, p, true), i)  => (k, p + (names(i).toString -> sv(vals(i))), true)
      }
    )
    TresqlUri.Uri(value, key.reverse, params, keyInPath)
  }

  // akka http uri methods
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

  def uriWithKey(uri: Uri, key: Seq[Any], keyInPath: Boolean): Uri =
    if (keyInPath) uriWithKeyInPath(uri, key) else uriWithKeyInQuery(uri, key)

  def uri(value: TresqlUri.Uri): Uri = {
    require(value.value != null, "Uri value must not be null!")
    val uriRegex = """(?U)(https?://[^/]+)?(?:(?:$)|(.+))?""".r
    val uriRegex(uriStart, uriPath) = value.value
    val path = Option(uriPath).map(Path(_)).getOrElse(Path.Empty)
    val nonNullParams = value.params.map { case (k, v) => (k, if (v == null) "" else v) }
    val uriWithoutKey =
      Option(uriStart).map(Uri(_)).getOrElse(Uri.Empty)
        .withPath(path)
        .withQuery(Query(nonNullParams))
    uriWithKey(uriWithoutKey, value.key.toVector, value.keyInPath)
  }
}
