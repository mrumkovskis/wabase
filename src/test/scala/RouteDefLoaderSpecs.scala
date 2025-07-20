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
}
