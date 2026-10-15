package org.wabase

import org.wabase.client.WabaseHttpClient

class BusinessScenarioValueCaptureSpecs extends BusinessScenariosBaseSpecs() {
  override def beforeAll() = ()
  override def afterAll() = ()
  override def initHttpClient: WabaseHttpClient = null

  private def check(response: Any, expected: Any) =
    assertResponse(response, expected, "[ROOT]", fullCompare = true)

  private def failMsg(response: Any, expected: Any): String =
    intercept[RuntimeException] { check(response, expected) }.getMessage

  it should "capture value without comparison" in {
    check("anything", "-> key") shouldBe Map("key" -> "anything")
    check("anything", "->key") shouldBe Map("key" -> "anything")
    check("anything", "  ->  key  ") shouldBe Map("key" -> "anything")
    check(null, "-> key") shouldBe Map("key" -> null)
    check(List(1, 2), "-> key") shouldBe Map("key" -> List(1, 2))
  }

  it should "capture value when it is equal to expected" in {
    check("abc", "-> key = abc") shouldBe Map("key" -> "abc")
    check("abc", "->key=abc") shouldBe Map("key" -> "abc")
    check("abc", "  ->  key  =  abc  ") shouldBe Map("key" -> "abc")
    check(12, "-> size = 12") shouldBe Map("size" -> 12)
    check(Map("sha" -> "f0ca", "id" -> 1), Map("sha" -> "-> sha = f0ca", "id" -> "-> id")) shouldBe
      Map("sha" -> "f0ca", "id" -> 1)
    check(List("a", "b"), List("-> first = a", "b")) shouldBe Map("first" -> "a")
  }

  it should "capture value of a check" in {
    check("abc", "-> key = not_null()") shouldBe Map("key" -> "abc")
    check(List(1, 2, 3), "-> key = size(3)") shouldBe Map("key" -> List(1, 2, 3))
    check(List(1, 2, 3), "-> key = size(>=2)") shouldBe Map("key" -> List(1, 2, 3))
  }

  it should "fail when value is not equal to expected" in {
    failMsg("abc", "-> key = xyz") shouldBe "[ROOT]: Element abc should be equal to xyz"
    failMsg(null, "-> key = xyz") shouldBe "[ROOT]: Element null should be equal to xyz"
    failMsg(Map("sha" -> "f0ca"), Map("sha" -> "-> sha = dead")) shouldBe "[ROOT]/sha: Element f0ca should be equal to dead"
    failMsg(null, "-> key = not_null()") shouldBe "[ROOT]: Element null should not be null"
    failMsg(List(1, 2), "-> key = size(3)") shouldBe "[ROOT]: Array size 2 should match size(3)"
  }

  it should "compare data containing an arrow" in {
    check("a -> b", "a -> b") shouldBe Map.empty
    check(Map("route" -> "a -> b -> c"), Map("route" -> "a -> b -> c")) shouldBe Map.empty
    check("a -> b", "-> key = a -> b") shouldBe Map("key" -> "a -> b")
    failMsg("a -> b", "a -> c") shouldBe "[ROOT]: Element a -> b should be equal to a -> c"
  }

  it should "compare data starting with an arrow when captured" in {
    check("-> b", "-> _ = -> b") shouldBe Map("_" -> "-> b")
    failMsg("-> b", "-> _ = -> c") shouldBe "[ROOT]: Element -> b should be equal to -> c"
  }

  it should "fail on invalid capture" in {
    failMsg("abc", "->") shouldBe "[ROOT]: Invalid capture '->', expected '-> name' or '-> name = <expectation>'"
    failMsg("abc", "-> key =") shouldBe "[ROOT]: Invalid capture '-> key =', expected '-> name' or '-> name = <expectation>'"
    failMsg("abc", "-> key name") shouldBe
      "[ROOT]: Invalid capture '-> key name', expected '-> name' or '-> name = <expectation>'"
  }
}
