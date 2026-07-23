package org.wabase

import org.apache.pekko.http.scaladsl.model.Uri
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.tresql.{Resources, Query => TresqlQuery}

import java.sql.{Connection, DriverManager}
import scala.collection.immutable.{ListMap, Seq}

class TresqlUriSpecs extends AnyFlatSpec with Matchers {

  it should "create uri from tresql" in {
    DbDrivers.loadDrivers
    var conn: Connection = DriverManager.getConnection("jdbc:hsqldb:mem:tresql_uri")
    val res = new Resources{}.withConn(conn).withDialect(org.tresql.dialects.HSQLDialect)
    val tresql_uris = List[(String, Map[String, Any], TresqlUri.Uri, String)](
      ("{ 'path1', 'path2', '?', 'value1' param1, 'value2' param2 }",
        Map(),
        TresqlUri.Uri(Seq("path1", "path2"), Nil, ListMap("param1" -> "value1", "param2" -> "value2")),
        "path1/path2?param1=value1&param2=value2",
      ),
      ("{ :path1?, :path2?, '?', :param1? param1, :param2? param2 }",
        Map("path2" -> "path2", "param1" -> "value1"),
        TresqlUri.Uri(Seq("path2"), Nil, ListMap("param1" -> "value1")),
        "path2?param1=value1",
      ),
      ("{ :path1?, :path2?, :path3?, '?', :param1? param1, :param2? param2 }",
        Map("path2" -> "path2", "path3" -> "path3"),
        TresqlUri.Uri(Seq("path2", "path3"), Nil, ListMap()),
        "path2/path3",
      ),
      ("{ 'glāžšķūņu rūķīši', 'rūķīši', :path3?, '?', 'āīū' žčņ, 'ēšģ' ķļŗ }",
        Map(),
        TresqlUri.Uri(Seq("glāžšķūņu rūķīši", "rūķīši"), Nil, ListMap("žčņ" -> "āīū", "ķļŗ" -> "ēšģ")),
        "glāžšķūņu rūķīši/rūķīši?žčņ=āīū&ķļŗ=ēšģ",
      ),
    )
    tresql_uris foreach { case (uriTresql, bind_vars, truri, uri) =>
      val turi = new TresqlUri().tresqlUriValue(TresqlUri.Tresql(uriTresql))(TresqlQuery, bind_vars, res)
      turi shouldBe truri
      val url = new TresqlUri().uri(turi)
      WabaseService.toReadableString(url.path) + {
        val q = url.query().map { case (k, v) => s"$k=$v"}.mkString("&")
        if (q.isEmpty) "" else s"?$q"
      } shouldBe uri
    }
  }

  it should "encode key in query or path according to keyInQuery" in {
    val base = Uri("data/person")
    val key = Seq("42", "a/b")

    val inQuery = new TresqlUri(keyInQuery = true)
    inQuery.uriWithKey(base, key).toString shouldBe "data/person?/42/a%2Fb"
    inQuery.uri(TresqlUri.Uri(Seq("data/person"), key, ListMap("x" -> "1"))).toString shouldBe
      "data/person?/42/a%2Fb?x=1"

    val inPath = new TresqlUri(keyInQuery = false)
    inPath.uriWithKey(base, key).toString shouldBe "data/person/42/a%2Fb"
    inPath.uri(TresqlUri.Uri(Seq("data/person"), key, ListMap("x" -> "1"))).toString shouldBe
      "data/person/42/a%2Fb?x=1"
  }
}
