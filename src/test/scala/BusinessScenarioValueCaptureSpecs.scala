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

  it should "capture value when it is equal to expected" in {
    check("abc", "abc -> key") shouldBe Map("key" -> "abc")
    check("abc", "abc->key") shouldBe Map("key" -> "abc")
    check("abc", "  abc  ->  key  ") shouldBe Map("key" -> "abc")
    check(12, "12 -> size") shouldBe Map("size" -> 12)
    check(Map("sha" -> "f0ca", "id" -> 1), Map("sha" -> "f0ca -> sha", "id" -> "-> id")) shouldBe
      Map("sha" -> "f0ca", "id" -> 1)
    check(List("a", "b"), List("a -> first", "b")) shouldBe Map("first" -> "a")
  }

  it should "keep capture without comparison when nothing precedes the arrow" in {
    check("anything", "-> key") shouldBe Map("key" -> "anything")
    check(null, "-> key") shouldBe Map("key" -> null)
  }

  it should "fail when value is not equal to expected" in {
    failMsg("abc", "xyz -> key") shouldBe "[ROOT]: Element abc should be equal to xyz"
    failMsg(null, "xyz -> key") shouldBe "[ROOT]: Element null should be equal to xyz"
    failMsg(Map("sha" -> "f0ca"), Map("sha" -> "dead -> sha")) shouldBe "[ROOT]/sha: Element f0ca should be equal to dead"
  }
}
