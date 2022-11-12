package org.wabase

import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.Uri.{Path, Query}
import org.tresql.RowLike
import org.tresql.ast.{Col, Cols, Exp, StringConst}
import org.tresql.parsing.QueryParsers

import java.net.URLEncoder
import scala.collection.immutable.ListMap

object TresqlUri {
  case class Tresql(uriTresql: String, queryStringColIdx: Int)
  case class Uri(value: String, key: Seq[Any] = Nil, params: ListMap[String, String] = ListMap())
}

class TresqlUri {
  def queryStringColIdx(tresqlUriExp: Exp)(parser: QueryParsers): Int =
    parser.traverser[Int](_ => {
      case Cols(_, cols) => cols indexWhere {
        _ == Col(StringConst("?"), null)
      }
    })(-1)(tresqlUriExp)

  def uriValue(row: RowLike, startIdx: Int, queryStringColIdx: Int): TresqlUri.Uri = {
    val colCount = row.columnCount
    val pi = if (queryStringColIdx == -1) colCount else queryStringColIdx
    val (value, (key, params)) = (row.string(startIdx),
      ((startIdx + 1) until colCount).foldLeft(List[String]() -> ListMap[String, String]()) {
        case ((k, p), i) if i < pi => (row.string(i) :: k, p)
        case ((k, p), i) if i > pi => (k, p + (row.column(i).name -> row.string(i)))
        case (r, _) => r // i == pi - parameter separator - '?'
      }
    )
    TresqlUri.Uri(value, key.reverse, params)
  }

  // akka http uri methods
  def keyToUriStrings(key: Seq[Any]): Seq[String] = key.map {
    case t: java.sql.Timestamp => t.toLocalDateTime.toString.replace('T', '_')
    case t: java.time.LocalDateTime => t.toString.replace('T', '_')
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

  /** Override to change key representation in redirect uri,
    * see uriWithKeyInPath(uri, key) and uriWithKeyInQuery(uri, key).
    * Default is uriWithKeyInQuery.
    */
  def uriWithKey(uri: Uri, key: Seq[Any]) =
    uriWithKeyInQuery(uri, key)

  def uri(value: TresqlUri.Uri): Uri = {
    require(value.value != null, "Uri value must not be null!")
    val uriRegex = """(?U)(https?://[^/]+)?(?:(?:$)|(.+))?""".r
    val uriRegex(uriStart, uriPath) = value.value
    val path = Option(uriPath).map(Path(_)).getOrElse(Path.Empty)
    val nonNullParams = value.params.map { case (k, v) => (k, if (v == null) "" else v) }
    Option(uriStart).map(Uri(_)).getOrElse(Uri.Empty)
      .withPath(path)
      .withQuery(Query(nonNullParams))
  }
}
