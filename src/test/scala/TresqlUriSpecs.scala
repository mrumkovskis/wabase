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

  it should "parse uri syntax with hyphenated words" in {
    DbDrivers.loadDrivers
    val conn: Connection = DriverManager.getConnection("jdbc:hsqldb:mem:tresql_uri_parser")
    val res = new Resources{}.withConn(conn).withDialect(org.tresql.dialects.HSQLDialect)
    val parser = new TresqlUriParsers {}
    def parse(uri: String) = parser.parseAll(parser.uriParser, uri) match {
      case parser.Success(r, _) => r.tresql
      case f => fail(f.toString)
    }
    val uris = List[(String, Map[String, Any], TresqlUri.Uri)](
      ("api/order-items/items?page-size=10&sort=created-at",
        Map(),
        TresqlUri.Uri(Seq("api", "order-items", "items"), Nil, ListMap("page-size" -> "10", "sort" -> "created-at")),
      ),
      // absolute path
      ("/a/:id",
        Map("id" -> 5),
        TresqlUri.Uri(Seq("/a", "5"), Nil, ListMap()),
      ),
      ("/:x/y",
        Map("x" -> "b"),
        TresqlUri.Uri(Seq("/b", "y"), Nil, ListMap()),
      ),
      ("/order-items?x=1",
        Map(),
        TresqlUri.Uri(Seq("/order-items"), Nil, ListMap("x" -> "1")),
      ),
      // braces keep variable mandatory, '?' followed by variable or string is parsed as outer join marker
      ("a/(:nr)?:owner",
        Map("nr" -> 7, "owner" -> "o"),
        TresqlUri.Uri(Seq("a", "7"), Nil, ListMap("owner" -> "o")),
      ),
      ("a/(:x || 'y')?'o-p'=1",
        Map("x" -> "x"),
        TresqlUri.Uri(Seq("a", "xy"), Nil, ListMap("o-p" -> "1")),
      ),
      ("a/b?",
        Map(),
        TresqlUri.Uri(Seq("a", "b"), Nil, ListMap()),
      ),
      ("a/v2-beta-3/b",
        Map(),
        TresqlUri.Uri(Seq("a", "v2-beta-3", "b"), Nil, ListMap()),
      ),
      ("a/:id-1?x-y=:id-1&'q-r'=s",
        Map("id" -> 5),
        TresqlUri.Uri(Seq("a", "4"), Nil, ListMap("x-y" -> "4", "q-r" -> "s")),
      ),
      // '?' consumed by expr parser as outer join marker when not followed by ident
      ("path?'page-size'=1",
        Map(),
        TresqlUri.Uri(Seq("path"), Nil, ListMap("page-size" -> "1")),
      ),
      ("a/path?:x&y=2",
        Map("x" -> "v"),
        TresqlUri.Uri(Seq("a", "path"), Nil, ListMap("x" -> "v", "y" -> "2")),
      ),
      ("http_test_2/:name?:manipulation_date&:vaccine&id=:health_id.id",
        Map("name" -> "n", "manipulation_date" -> "2024-01-01", "vaccine" -> "v", "health_id" -> Map("id" -> 7)),
        TresqlUri.Uri(Seq("http_test_2", "n"), Nil,
          ListMap("manipulation_date" -> "2024-01-01", "vaccine" -> "v", "id" -> "7")),
      ),
    )
    uris foreach { case (uri, bindVars, truri) =>
      withClue(uri) {
        new TresqlUri().tresqlUriValue(TresqlUri.Tresql(parse(uri)))(TresqlQuery, bindVars, res) shouldBe truri
      }
    }
    // braces keep minus operation
    parse("a/(b-c)") should include("(b - c)")
    // leading slash is part of the first segment, braces segment is not rendered as join
    def n(s: String) = s.replaceAll("\\s+", "")
    n(parse("/a/:id")) shouldBe n("null{'/a', :id}")
    n(parse("/:x/y")) shouldBe n("null{'/' || :x, 'y'}")
    n(parse("a/(:nr)?:owner")) shouldBe n("null{'a', (:nr), '?', :owner owner}")
    // last segment variable is mandatory if '?' is followed by query parameters, '??' keeps it optional
    n(parse("a/:id")) shouldBe n("null{'a', :id}")
    n(parse("a/:id?")) shouldBe n("null{'a', :id?}")
    n(parse("a/:id?x=1")) shouldBe n("null{'a', :id, '?', 1 x}")
    n(parse("a/:id??x=1")) shouldBe n("null{'a', :id?, '?', 1 x}")
    n(parse("a/:id??")) shouldBe n("null{'a', :id?}")
    n(parse("a/:id?/b?x=1")) shouldBe n("null{'a', :id?, 'b', '?', 1 x}")
    def url(uri: String, bindVars: Map[String, Any]) =
      new TresqlUri().uri(new TresqlUri().tresqlUriValue(TresqlUri.Tresql(parse(uri)))(TresqlQuery, bindVars, res)).toString
    url("/a/:id", Map("id" -> 5)) shouldBe "/a/5"
    url("/a/:id?", Map()) shouldBe "/a"
    url("/a/:id??x=1", Map()) shouldBe "/a?x=1"
    url("/a/:id??x=1", Map("id" -> 5)) shouldBe "/a/5?x=1"
    url("/a/:id?x=1", Map("id" -> 5)) shouldBe "/a/5?x=1"
    intercept[Exception](url("/a/:id?x=1", Map()))
    url("/order-items?x=1", Map()) shouldBe "/order-items?x=1"
    url("/http_forest/(:nr)?:owner&'o-p'=1", Map("nr" -> "N1", "owner" -> "O")) shouldBe "/http_forest/N1?owner=O&o-p=1"
  }

  it should "parse uri in http, redirect and status operations" in {
    import AppMetadata.Action.{Http, Response, Tresql}
    val p = new OpParser("uri_test", null, getClass.getClassLoader)
    def op(s: String) = p.parseOperation(s)
    // compare ignoring whitespace
    def n(s: String) = s.replaceAll("\\s+", "")
    def uri(s: String) = n(op(s) match {
      case h: Http => h.uriTresql.uriTresql
      case Response(_, _, _, Tresql(t, _, _)) => t
      case x => fail(s"Unexpected operation: $x")
    })
    uri("http a/:id?x=1") shouldBe n("null{'a', :id, '?', 1 x}")
    uri("http get a/'?/'/:id") shouldBe n("null{'a', '?/', :id}")
    uri("http (a/b)") shouldBe n("null{'a', 'b'}")
    uri("http [client] /a/b") shouldBe n("null{'/a', 'b'}")
    op("http /a/b :h") match {
      case h: Http =>
        n(h.uriTresql.uriTresql) shouldBe n("null{'/a', 'b'}")
        h.headerTresql.tresql shouldBe ":h"
      case x => fail(s"Unexpected operation: $x")
    }
    // column list syntax
    uri("http {'/p', :nr, '?', :id id}") shouldBe n("null{'/p', :nr, '?', :id id}")
    uri("http ({'/p', '?', :id id})") shouldBe n("null{'/p', '?', :id id}")
    uri("http config[name = 'svc']{base_url, 'items', '?', :id id}") shouldBe
      n("config[name = 'svc']{base_url, 'items', '?', :id id}")
    // named arguments after uri
    op("http (a/b) headers = {x}") match {
      case h: Http =>
        n(h.uriTresql.uriTresql) shouldBe n("null{'a', 'b'}")
        n(h.headerTresql.tresql) shouldBe n("null{x}")
      case x => fail(s"Unexpected operation: $x")
    }
    op("http post a/'b' body = :x") match {
      case h: Http => n(h.uriTresql.uriTresql) shouldBe n("null{'a', 'b'}")
      case x => fail(s"Unexpected operation: $x")
    }
    intercept[RuntimeException](op("http a/b headers = {x}")).getMessage should include("enclosed in parentheses")
    intercept[RuntimeException](op("http a/b?x=y headers = {x}")).getMessage should include("enclosed in parentheses")
    // body or filter after uri would be parsed as query columns or filter
    intercept[RuntimeException](op("http post /a { :name name } { 'Content-Type', 'x' }"))
      .getMessage should include("enclosed in parentheses")
    intercept[RuntimeException](op("http get a/b [:x]")).getMessage should include("enclosed in parentheses")
    intercept[RuntimeException](op("http post a?x=b { :name name }")).getMessage should include("enclosed in parentheses")
    op("http post (/a) { :name name } { 'Content-Type', 'x' }") match {
      case h: Http =>
        n(h.uriTresql.uriTresql) shouldBe n("null{'/a'}")
        n(h.body.asInstanceOf[Tresql].tresql) shouldBe n("null{:name name}")
        n(h.headerTresql.tresql) shouldBe n("null{'Content-Type', 'x'}")
      case x => fail(s"Unexpected operation: $x")
    }
    // subquery in braces is allowed
    uri("http a/(b[id = :id]{name})?x=(c{count(*)})") shouldBe n("null{'a', (b[id = :id]{name}), '?', (c{count(*)}) x}")
    // redirect and status
    uri("redirect a/'?/'/:id?x=1") shouldBe n("null{'a', '?/', :id, '?', 1 x}")
    uri("redirect {'data/path', '?', :id id}") shouldBe n("null{'data/path', '?', :id id}")
    uri("status 303 a/(:id)?x=1") shouldBe n("null{'a', (:id), '?', 1 x}")
    uri("status 303 { 'data/path', '?/', :id, '?', 'v' par1 }") shouldBe n("null{'data/path', '?/', :id, '?', 'v' par1}")
    // non redirection status body is not uri
    uri("status ok :x") shouldBe n(":x")
    uri("status ok { :uri || 'about' }") shouldBe n("null{:uri || 'about'}")
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
