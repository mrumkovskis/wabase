package org.wabase

import org.wabase.client.WabaseHttpClient

class BusinessScenarioContainsCheckSpecs extends BusinessScenariosBaseSpecs() {
  override def beforeAll() = ()
  override def afterAll() = ()
  override def initHttpClient: WabaseHttpClient = null

  private def check(response: Any, expected: Any, fullCompare: Boolean = true) =
    assertResponse(response, expected, "[ROOT]", fullCompare)

  private def failMsg(response: Any, expected: Any, fullCompare: Boolean = true): String =
    intercept[RuntimeException] { check(response, expected, fullCompare) }.getMessage

  it should "accept listed elements in any order and allow further elements" in {
    check(List("guest", "admin", "owner"), Map("contains()" -> List("admin", "guest"))) shouldBe Map.empty
    check(List("a", "b", "a"), Map("contains()" -> List("a", "a"))) shouldBe Map.empty
    check(Nil, Map("contains()" -> Nil)) shouldBe Map.empty
    check(List(1), Map("contains()" -> Nil)) shouldBe Map.empty
    check(List("a", null, "b"), Map("contains()" -> List(null, "b"))) shouldBe Map.empty
    check(java.util.Arrays.asList("b", "a", "c"), Map("contains()" -> List("a", "c"))) shouldBe Map.empty
    check(Array("b", "a"), Map("contains()" -> List("a"))) shouldBe Map.empty
    check(List("b", "a"), Map("contains( )" -> List("b"))) shouldBe Map.empty
  }

  it should "check contains() at field and list-element level" in {
    check(
      Map("roles" -> List("guest", "admin", "owner")),
      Map("roles" -> Map("contains()" -> List("admin", "guest"))),
    ) shouldBe Map.empty
    check(
      List(List("b", "a", "c"), "x"),
      List(Map("contains()" -> List("a", "b")), "x"),
    ) shouldBe Map.empty
    check(
      List(List("b", "a", "c"), List(1), "x"),
      Map("contains()" -> List(Map("contains()" -> List("a", "b")), "x")),
    ) shouldBe Map.empty
    check(
      List(List(1, 2), "x", Nil),
      Map("contains()" -> List("size(2)", "size(0)")),
    ) shouldBe Map.empty
  }

  it should "match object elements and keep full_compare for their fields" in {
    check(
      List(
        Map("code" -> "p2", "name" -> "Jane"),
        Map("code" -> "p3", "name" -> "Jim"),
        Map("code" -> "p1", "name" -> "John"),
      ),
      Map("contains()" -> List(
        Map("code" -> "p1", "name" -> "John"),
        Map("code" -> "p2", "name" -> "Jane"),
      )),
    ) shouldBe Map.empty
    check(
      List(Map("code" -> "p1", "name" -> "John", "surname" -> "Doe")),
      Map("contains()" -> List(Map("code" -> "p1"))),
      fullCompare = false,
    ) shouldBe Map.empty
    failMsg(
      List(Map("code" -> "p1", "name" -> "John")),
      Map("contains()" -> List(Map("code" -> "p1"))),
    ) should include ("Array should contain")
    check(
      List(Map("id" -> 1), Map("id" -> 1)),
      Map("contains()" -> List(Map("id" -> 1), Map("id" -> 1))),
    ) shouldBe Map.empty
    failMsg(
      List(Map("id" -> 1)),
      Map("contains()" -> List(Map("id" -> 1), Map("id" -> 1))),
    ) should include ("Array should contain all of")
  }

  it should "try other assignments when a broad pattern consumes an element" in {
    check(
      List(1, 2),
      Map("contains()" -> List("not_null()", 1)),
    ) shouldBe Map.empty
    failMsg(
      List(1),
      Map("contains()" -> List("not_null()", 1)),
    ) shouldBe "[ROOT]: Array should contain all of List(not_null(), 1). Response: List(1)"
    check(
      List(Map("id" -> 1, "name" -> "a"), Map("id" -> 2, "name" -> "b")),
      Map("contains()" -> List(
        Map("id" -> "-> picked = not_null()"),
        Map("id" -> 1, "name" -> "a"),
      )),
      fullCompare = false,
    ) shouldBe Map("picked" -> 2)
  }

  it should "capture the whole array and captures from matched elements" in {
    check(
      List("b", "a", "c"),
      Map("-> captured = contains()" -> List("a", "c")),
    ) shouldBe Map("captured" -> List("b", "a", "c"))
    check(
      List("b", "a"),
      Map("  -> captured_roles = contains()  " -> List("a")),
    ) shouldBe Map("captured_roles" -> List("b", "a"))
    check(
      List(List(1, 2, 3), "x"),
      Map("contains()" -> List("-> nums = size(3)")),
    ) shouldBe Map("nums" -> List(1, 2, 3))
    check(
      List(1),
      Map("-> item = contains()" -> List("-> item = not_null()")),
    ) shouldBe Map("item" -> List(1))
  }

  it should "fail when an element is missing or the value is not an array" in {
    failMsg(
      List("guest", "owner"),
      Map("contains()" -> List("admin")),
    ) shouldBe "[ROOT]: Array should contain admin. Response: List(guest, owner)"
    failMsg(
      Map("roles" -> List("guest")),
      Map("roles" -> Map("contains()" -> List("admin"))),
    ) shouldBe "[ROOT]/roles: Array should contain admin. Response: List(guest)"
    failMsg(
      List("a", "b"),
      Map("contains()" -> List("c")),
      fullCompare = false,
    ) shouldBe "[ROOT]: Array should contain c. Response: List(a, b)"
    failMsg("x", Map("contains()" -> List("x"))) shouldBe
      "[ROOT]: Element x should be an array to match contains()"
    failMsg(null, Map("contains()" -> Nil)) shouldBe
      "[ROOT]: Element null should be an array to match contains()"
    failMsg(1, Map("contains()" -> List(1))) shouldBe
      "[ROOT]: Element 1 should be an array to match contains()"
    failMsg(Map("a" -> 1), Map("contains()" -> List(1)), fullCompare = false) shouldBe
      "[ROOT]: Object should contain key: contains()"
    failMsg(List(1), Map("contains()" -> 1)) shouldBe
      "[ROOT]: contains() value 1 should be a list"
    failMsg(List(1), Map("contains()" -> null)) shouldBe
      "[ROOT]: contains() value null should be a list"
  }

  it should "keep exact list compare for an object field named contains()" in {
    failMsg(
      Map("contains()" -> List("admin", "guest", "owner")),
      Map("contains()" -> List("admin", "guest")),
    ) shouldBe "[ROOT]/contains(): List size 3 should be equal to 2"
    check(
      Map("contains()" -> List("admin", "guest")),
      Map("contains()" -> List("admin", "guest")),
    ) shouldBe Map.empty
  }
}
