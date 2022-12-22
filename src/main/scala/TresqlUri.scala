package org.wabase

import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.Uri.{Path, Query}
import org.tresql.{Resources, RowLike, SingleValueResult, Query => TresqlQuery}
import org.tresql.ast.{Col, Cols, Const, Exp, Null, Obj, StringConst, Variable}
import org.tresql.parsing.QueryParsers

import java.net.URLEncoder
import scala.collection.immutable.{ListMap, Seq}

object TresqlUri {
  sealed trait TrUri
  case class Tresql(uriTresql: String, queryStringColIdx: Int) extends TrUri
  case class PrimitiveTresql(hostInitPath: String,
                             path: Seq[String],
                             params: ListMap[String, String],
                             origin: String) extends TrUri
  case class Uri(value: String, key: Seq[Any] = Nil, params: ListMap[String, String] = ListMap())
}

class TresqlUri {
  def queryStringColIdx(tresqlUriExp: Exp)(parser: QueryParsers): Int = {
    if (tresqlUriExp == null) -1 else
      parser.traverser[Int](_ => {
        case Cols(_, cols) => cols indexWhere {
          _ == Col(StringConst("?"), null)
        }
      })(-1)(tresqlUriExp)
  }

  def parse(exp: Exp)(parser: QueryParsers): TresqlUri.TrUri = {
    lazy val traverser = parser.traverser[(TresqlUri.TrUri, Int, Int)] { tru =>
      def calcPv(e: Exp, a: String) = {
        val (pt, idx, paramIdx) = tru
        val ptru =
          if (pt == null) TresqlUri.PrimitiveTresql(null, Nil, ListMap(), exp.tresql)
          else pt.asInstanceOf[TresqlUri.PrimitiveTresql]
        val nptru = {
          if (idx == -1) ptru.copy(hostInitPath = e.tresql)
          else if (idx < paramIdx) ptru.copy(path = ptru.path ++ Seq(e.tresql))
          else ptru.copy(params = ptru.params + (a -> e.tresql))
        }
        (nptru, idx + 1, paramIdx)
      }
      {
        case o: Obj if tru._1 == null && o.obj == Null && o.join == null =>
          (TresqlUri.PrimitiveTresql(null, Nil, ListMap(), exp.tresql), tru._2, tru._3)
        case _: Obj =>
          (TresqlUri.Tresql(exp.tresql, tru._3), tru._2, tru._3)
        case Col(StringConst("?"), _) => (tru._1, tru._2 + 1, tru._2 + 1)
        case Col(c: Const, a) if tru._1 == null || tru._1.isInstanceOf[TresqlUri.PrimitiveTresql] =>
          calcPv(c, a)
        case Col(v: Variable, a) if tru._1 == null || tru._1.isInstanceOf[TresqlUri.PrimitiveTresql] =>
          calcPv(v, a)
        case c: Const if tru._1 == null || tru._1.isInstanceOf[TresqlUri.PrimitiveTresql] =>
          calcPv(c, null)
        case _: Col =>
          val ntru =
            if (tru._1.isInstanceOf[TresqlUri.PrimitiveTresql]) TresqlUri.Tresql(exp.tresql, tru._3)
            else tru._1
          (ntru, tru._2 + 1, tru._3)
      }
    }
    traverser((null, -1, Integer.MAX_VALUE))(exp) match {
      case (p: TresqlUri.PrimitiveTresql, _, _) => p
      case (t: TresqlUri.Tresql, _, idx) =>
        t.copy(queryStringColIdx = if (idx == Integer.MAX_VALUE) -1 else idx)
    }
  }

  def tresqlUriValue(trUri: TresqlUri.TrUri)(
    q: TresqlQuery, env: Map[String, Any], res: Resources): TresqlUri.Uri = trUri match {
    case TresqlUri.Tresql(t, idx) => uriValue(q(t, env)(res).unique, 0, idx)
    case TresqlUri.PrimitiveTresql(hostInitPath, key, params, _) =>
      def getVal(valTresql: String) = q(valTresql, env)(res).unique match {
        case SingleValueResult(v) => v
        case x => sys.error(s"Unexpected result for uri component '$valTresql': $x")
      }
      val value = String.valueOf(getVal(hostInitPath))
      val keyVal = key map getVal
      val paramsVal: ListMap[String, String] =
        params.map { case (k, v) => (k, String.valueOf(getVal(v))) }
      TresqlUri.Uri(value, keyVal, paramsVal)
  }

  def uriValue(row: RowLike, startIdx: Int, queryStringColIdx: Int): TresqlUri.Uri = {
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
    val pi = if (queryStringColIdx == -1) colCount else queryStringColIdx
    val (value, (key, params)) = (sv(vals(startIdx)),
      ((startIdx + 1) until colCount).foldLeft(List[String]() -> ListMap[String, String]()) {
        case ((k, p), i) if i < pi => (sv(vals(i)) :: k, p)
        case ((k, p), i) if i > pi => (k, p + (names(i).toString -> sv(vals(i))))
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
  def uriWithKey(uri: Uri, key: Seq[Any]): Uri =
    uriWithKeyInQuery(uri, key)

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
    uriWithKey(uriWithoutKey, value.key.toVector)
  }
}
