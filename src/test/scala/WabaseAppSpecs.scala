package org.wabase

import org.mojoz.metadata.ViewDef

import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers


object TestableWabaseApp extends TestApp {
  override def extractNamesFromOrderBy(orderBy: String) =
    super.extractNamesFromOrderBy(orderBy)
  override def stableOrderBy(viewDef: ViewDef, orderBy: String): String =
    super.stableOrderBy(viewDef, orderBy)
}

class WabaseAppSpecs extends FlatSpec with Matchers {

  import TestableWabaseApp._

  "wabase app" should "analyze orderBy" in {
    extractNamesFromOrderBy(null)             shouldBe Nil
    extractNamesFromOrderBy("")               shouldBe Nil
    extractNamesFromOrderBy("name")           shouldBe Seq("name")
    extractNamesFromOrderBy("null name")      shouldBe Seq("name")
    extractNamesFromOrderBy("name null")      shouldBe Seq("name")
    extractNamesFromOrderBy("null ~name")     shouldBe Seq("name")
    extractNamesFromOrderBy("~name null")     shouldBe Seq("name")

    extractNamesFromOrderBy("null  name ")    shouldBe Seq("name")
    extractNamesFromOrderBy("name  null ")    shouldBe Seq("name")
    extractNamesFromOrderBy(" null  ~ name ") shouldBe Seq("name")
    extractNamesFromOrderBy(" ~ name  null ") shouldBe Seq("name")

    extractNamesFromOrderBy("a,b,c,d")                           shouldBe Seq("a", "b", "c", "d")
    extractNamesFromOrderBy("~a null, null b, c, ~d")            shouldBe Seq("a", "b", "c", "d")
    extractNamesFromOrderBy("~a null, null b, c, ~d")            shouldBe Seq("a", "b", "c", "d")
    extractNamesFromOrderBy(" null  a  ,  ~b  null,~c,d  null")  shouldBe Seq("a", "b", "c", "d")
  }

  it should "compose stable orderBy" in {
    def v(orderBy: String*) =
      ViewDef("test", null, null, null, Nil, Nil, Nil, Nil, orderBy.toVector, null, null, Nil, Nil, Map.empty)
    stableOrderBy(v(), null)                          shouldBe null
    stableOrderBy(v(), "a")                           shouldBe "a"
    stableOrderBy(v(), "a, b")                        shouldBe "a, b"
    stableOrderBy(v("a", "b", "c", "d"), "a")         shouldBe "a, b, c, d"
    stableOrderBy(v("a", "b", "c", "d"), "b")         shouldBe "b, a, c, d"
    stableOrderBy(v("a", "b", "c", "d"), "c")         shouldBe "c, a, b, d"
    stableOrderBy(v("a", "b", "c", "d"), "d")         shouldBe "d, a, b, c"
    stableOrderBy(v("a", "b", "c", "d"), "d, c")      shouldBe "d, c, a, b"
    stableOrderBy(v("a", "b", "c", "d"), "~d, c")     shouldBe "~d, c, a, b"
    stableOrderBy(v("a", "b", "c", "d"), "d,  c null")shouldBe "d,  c null, a, b"
    stableOrderBy(v("b"), "a")                        shouldBe "a, b"
    stableOrderBy(v("~b"), "a")                       shouldBe "a, ~b"
    stableOrderBy(v("null ~b"), "a")                  shouldBe "a, null ~b"
    stableOrderBy(v("~b null"), "a")                  shouldBe "a, ~b null"
    stableOrderBy(v("~b null"), "~a")                 shouldBe "~a, ~b null"
    stableOrderBy(v("~b null"), "null ~a")            shouldBe "null ~a, ~b null"
    stableOrderBy(v("~b null"), "~a null")            shouldBe "~a null, ~b null"
    stableOrderBy(v("a", "b"), "a")                   shouldBe "a, b"
    stableOrderBy(v("~a", "b"), "a")                  shouldBe "a, b"
    stableOrderBy(v("a", "~b"), "a")                  shouldBe "a, ~b"
    stableOrderBy(v("a", "null ~b"), "a")             shouldBe "a, null ~b"
    stableOrderBy(v("a", "~b null"), "a")             shouldBe "a, ~b null"
    stableOrderBy(v("a", "~b null"), "~a")            shouldBe "~a, ~b null"
    stableOrderBy(v("a", "~b null"), "null ~a")       shouldBe "null ~a, ~b null"
    stableOrderBy(v("a", "~b null"), "~a null")       shouldBe "~a null, ~b null"
    stableOrderBy(v("null a", "~b null"), "~a null")  shouldBe "~a null, ~b null"
    stableOrderBy(v("a null", "~b null"), "~a null")  shouldBe "~a null, ~b null"
    stableOrderBy(v("null ~a", "~b null"), "~a null") shouldBe "~a null, ~b null"
    stableOrderBy(v("~a null", "~b null"), "~a null") shouldBe "~a null, ~b null"
    stableOrderBy(v("~a null", "~b null"), "~a  null")shouldBe "~a  null, ~b null"
  }
}
