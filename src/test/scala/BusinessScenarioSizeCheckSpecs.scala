package org.wabase

import org.wabase.client.WabaseHttpClient

class BusinessScenarioSizeCheckSpecs extends BusinessScenariosBaseSpecs() {
  override def beforeAll() = ()
  override def afterAll() = ()
  override def initHttpClient: WabaseHttpClient = null

  private def check(response: Any, expected: Any) =
    assertResponse(response, expected, "[ROOT]", fullCompare = true)

  private def failMsg(response: Any, expected: Any): String =
    intercept[RuntimeException] { check(response, expected) }.getMessage

  it should "accept size(n) for arrays of that length" in {
    check(List(1, 2, 3), "size(3)") shouldBe Map.empty
    check(Nil, "size(0)") shouldBe Map.empty
    check(List(1, 2, 3), "size( 3 )") shouldBe Map.empty
    check(List(1, 2, 3), "size(=3)") shouldBe Map.empty
    check(List(1, 2, 3), "size(==3)") shouldBe Map.empty
  }

  it should "accept comparison and range size checks" in {
    check(List(1, 2, 3), "size(>2)") shouldBe Map.empty
    check(List(1, 2), "size(>=2)") shouldBe Map.empty
    check(List(1), "size(<2)") shouldBe Map.empty
    check(List(1, 2), "size(<=2)") shouldBe Map.empty
    check(List(1), "size(!=0)") shouldBe Map.empty
    check(List(1), "size(<>0)") shouldBe Map.empty
    check(List(1, 2, 3), "size(2..4)") shouldBe Map.empty
    check(List(1, 2, 3), "size(3..3)") shouldBe Map.empty
    check(List(1, 2, 3), "size( 2 .. 4 )") shouldBe Map.empty
  }

  it should "check array size at field and list-element level" in {
    check(Map("items" -> List(1, 2)), Map("items" -> "size(2)")) shouldBe Map.empty
    check(
      Map("outer" -> Map("inner" -> List("a", "b", "c"))),
      Map("outer" -> Map("inner" -> "size(>2)")),
    ) shouldBe Map.empty
    check(List(List(1, 2), "x", Nil), List("size(2)", "x", "size(0)")) shouldBe Map.empty
  }

  it should "capture the array when size check has -> key" in {
    check(List(1, 2, 3), "size(3) -> captured") shouldBe Map("captured" -> List(1, 2, 3))
    check(Map("items" -> List(1, 2)), Map("items" -> "size(>=2) -> captured_items")) shouldBe
      Map("captured_items" -> List(1, 2))
  }

  it should "fail when size does not match or value is not an array" in {
    failMsg(List(1, 2), "size(3)") shouldBe "[ROOT]: Array size 2 should match size(3)"
    failMsg(List(1, 2, 3), "size(>5)") shouldBe "[ROOT]: Array size 3 should match size(>5)"
    failMsg(List(1), "size(2..4)") shouldBe "[ROOT]: Array size 1 should match size(2..4)"
    failMsg("not-an-array", "size(1)") shouldBe "[ROOT]: Element not-an-array should be an array to match size(1)"
    failMsg(Map("a" -> 1), "size(1)") shouldBe "[ROOT]: Element Map(a -> 1) should be an array to match size(1)"
    failMsg(null, "size(0)") shouldBe "[ROOT]: Element null should be an array to match size(0)"
  }
}
