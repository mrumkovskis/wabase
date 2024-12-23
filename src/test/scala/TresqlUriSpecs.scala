package org.wabase

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.tresql.{Resources, Query => TresqlQuery}

import java.sql.{Connection, DriverManager}
import scala.collection.immutable.ListMap

class TresqlUriSpecs extends AnyFlatSpec with Matchers {

  it should "create uri from tresql" in {
    DbDrivers.loadDrivers
    var conn: Connection = DriverManager.getConnection("jdbc:hsqldb:mem:tresql_uri")
    val res = new Resources{}.withConn(conn).withDialect(org.tresql.dialects.HSQLDialect)
    val tresql_uris = List[(String, Map[String, Any], TresqlUri.Uri, String)](
      ("{ 'path1', 'path2', '?', 'value1' param1, 'value2' param2 }",
        Map(),
        TresqlUri.Uri("path1", List("path2"), ListMap("param1" -> "value1", "param2" -> "value2")),
        "x"
      ),
      ("{ :path1?, :path2?, '?', :param1? param1, :param2? param2 }",
        Map("path2" -> "path2", "param1" -> "value1"),
        TresqlUri.Uri("path2", List(), ListMap("param1" -> "value1")),
        "x"
      ),
      ("{ :path1?, :path2?, :path3?, '?', :param1? param1, :param2? param2 }",
        Map("path2" -> "path2", "path3" -> "path3"),
        TresqlUri.Uri("path2", List("path3"), ListMap()),
        "x"
      )
    )
    tresql_uris foreach { case (uriTresql, bind_vars, truri, uri) =>
      val turi = new TresqlUri().tresqlUriValue(TresqlUri.Tresql(uriTresql))(TresqlQuery, bind_vars, res)
      turi shouldBe truri
      //TODO test http uris
      //new TresqlUri().uri(turi).toString() shouldBe uri
    }
  }
}
