package org.wabase

import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers


class RouteDefLoaderSpecs extends FlatSpec with Matchers {

  behavior of "YamlRouteDefLoader"

  it should "detect valid swagger path" in {
    SwaggerPath.isValid("""/users/{id}""")                      shouldBe true
    SwaggerPath.isValid("""^/users/{id}""")                     shouldBe false
    SwaggerPath.isValid("""/report.{format}""")                 shouldBe true
    SwaggerPath.isValid("""/""")                                shouldBe true
    SwaggerPath.isValid("""//""")                               shouldBe false
    SwaggerPath.isValid("""/users/{id""")                       shouldBe false // unbalanced brace
  }

  it should "extract path parameters from valid swagger path" in {
    SwaggerPath.extractPathParameters("""/users/{id}""")        shouldBe Seq("id")
    SwaggerPath.extractPathParameters("""/report.{format}""")   shouldBe Seq("format")
    SwaggerPath.extractPathParameters("""/""")                  shouldBe Seq()
    SwaggerPath.extractPathParameters("""/{a}/{b}/c{d}e""")     shouldBe Seq("a", "b", "d")
    SwaggerPath.extractPathParameters("""/static""")            shouldBe Seq()
  }

  it should "extract path names and parameters from regex" in {
    RegexPath.convert("/a|/b")          shouldBe Seq(("/a", Nil), ("/b", Nil))
    RegexPath.convert("/(a|b)")         shouldBe Seq(("/{p1}", Seq(("p1", "^a|b$"))))
    RegexPath.convert("/a.b")           shouldBe Seq(("/a{p1}b", Seq(("p1", "^.$"))))
    RegexPath.convert("/a\\.b")         shouldBe Seq(("/a.b", Nil))
    RegexPath.convert("/a/(\\w+)")      shouldBe Seq(("/a/{p1}", Seq(("p1", "^\\w+$"))))
    RegexPath.convert("/a/(\\w+)?")     shouldBe Seq(("/a/{p1}", Seq(("p1", "^(\\w+)?$"))))
    RegexPath.convert("/a(/\\w+)?")     shouldBe List(("/a", List()), ("/a/{p1}", List(("p1", "^\\w+$"))))
    RegexPath.convert("/(abc)/.+/.*")   shouldBe Seq(("/abc/{p1}/{p2}", Seq(("p1", "^.+$"), ("p2", "^.*$"))))
    RegexPath.convert("/abc/(.+)(.+)")  shouldBe Seq(("/abc/{p1}{p2}", Seq(("p1", "^.+$"), ("p2", "^.+$"))))
    RegexPath.convert("/data/(\\w+(?::(?:create|count))?)(/.+)?") shouldBe Seq(
      ("/data/{p1}", Seq(("p1", "^\\w+(?::(?:create|count))?$"))),
      ("/data/{p1}/{p2}", Seq(("p1", "^\\w+(?::(?:create|count))?$"), ("p2", "^.+$")))
    )
    RegexPath.convert("/data/(?<name>\\w+)/(?<id>\\d+)") shouldBe Seq(
      ("/data/{name}/{id}", Seq(("name", "^\\w+$"), ("id", "^\\d+$")))
    )
    RegexPath.convert("(/.+\\.(?:css|gif))") shouldBe Seq(("/{p1}", Seq(("p1", "^.+\\.(?:css|gif)$"))))
  }
}
