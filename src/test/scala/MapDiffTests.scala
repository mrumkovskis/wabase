package org.wabase

import MapUtils._

import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers


class MapDiffTests extends FlatSpec with Matchers {

  "flattenTree" should "work" in {
    flattenTree(Map()) should be(Map())
    flattenTree(Map("a"->"b")) should be(Map(List("a")->"b"))
    flattenTree(Map("a"->"b","c"->"d")) should be(Map(List("a")->"b",List("c")->"d"))
    flattenTree(Map("a"->1)) should be(Map(List("a")->1))

    flattenTree(Map("a"->Map("b"->"c"))) should be(Map(List("a","b")->"c"))
    flattenTree(Map("a"->Map("b"->"c"),"c"->"d")) should be(Map(List("a","b")->"c",List("c")->"d"))
    flattenTree(Map("a"->Map("b"-> Map("c"->"f")),"c"->"d")) should be(Map(List("a","b","c")->"f",List("c")->"d"))

    flattenTree(Map("a"->List("b"))) should be(Map(List("a",98)->"b"))
    flattenTree(Map("a"->List("b", "c"))) should be(Map(List("a",98)->"b",List("a",99)->"c"))
    val hash = Map("b"->"c", "m"->"n").hashCode()
    flattenTree(Map("a"->List(Map("b"->"c", "m"->"n")))) should be(Map(List("a",hash,"b")->"c",List("a",hash,"m")->"n"))
    flattenTree(Map("i" ->Map("a"->List(Map("b"->"c", "m"->"n"))))) should be(Map(List("i","a",hash,"b")->"c",List("i","a",hash,"m")->"n"))
  }

  "zipMaps" should "work" in {
    zipMaps(Map(),Map()) should be(Map())
    zipMaps(Map(List("a")->"b"),Map(List("a")->"c")) should be(Map(List("a")->("b", "c")))
    zipMaps(Map(List("a")->"b"),Map()) should be(Map(List("a")->("b", null)))
    zipMaps(Map(),Map(List("a")->"b")) should be(Map(List("a")->(null, "b")))
    zipMaps(Map(List("a")->"b"),Map(List("c")->"d")) should be(Map(List("a")->("b", null),List("c")->(null, "d")))
  }

  "diffMaps" should "work" in {
    diffMaps(Map(),Map()) should be(Map())
    diffMaps(Map("a"->"b"),Map("a"->"c")) should be(Map(List("a")->("b", "c")))
    diffMaps(Map("a"->"b"),Map()) should be(Map(List("a")->("b", null)))
    diffMaps(Map(),Map("a"->"b")) should be(Map(List("a")->(null, "b")))
    diffMaps(Map("a"->"b"),Map("a"->"b")) should be(Map())

    diffMaps(Map("a"->Map("b"-> Map("c"->"d"))),Map("a"->Map("b"-> Map("c"->"d")))) should be(Map())
    diffMaps(Map("a"->Map("b"-> Map("c"->"d"))),Map("a"->Map("b"-> Map("c"->"e")))) should be(Map(List("a","b","c")->("d", "e")))

    diffMaps(Map("a"->List("b", "c")),Map("a"->List("b", "c"))) should be(Map())
    diffMaps(Map("a"->List("b", "c")),Map("a"->List("b", "e"))) should be(Map(List("a",99)->("c", null),List("a",101)->(null, "e")))

    val hash1 = Map("c"->"d", "e"->"f").hashCode()
    val hash2 = Map("c"->"d1", "e"->"f").hashCode()
    diffMaps(Map("a"->List(Map("c"->"d", "e"->"f"), "c")),Map("a"->List(Map("c"->"d", "e"->"f"), "c"))) should be(Map())
    diffMaps(Map("a"->List(Map("c"->"d", "e"->"f"), "c")),Map("a"->List(Map("c"->"d1", "e"->"f"), "c"))) should be(
          Map(List("a",hash1,"c") -> ("d",null),
              List("a",hash1,"e") -> ("f",null),
              List("a",hash2,"c") -> (null,"d1"),
              List("a",hash2,"e") -> (null,"f")))
  }
}
