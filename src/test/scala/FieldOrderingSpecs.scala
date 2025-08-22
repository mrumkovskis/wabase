package org.wabase

import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.TreeMap

class FieldOrderingSpecs extends FlatSpec with Matchers {
  behavior of "FieldOrderingSpecs"

  object FieldOrderingTestApp extends AppBase[TestUsr] with NoAudit[TestUsr]
      with DbAccess with PostgreSqlConstraintMessage {
    object FieldOrdTestQuerease extends TestQuerease("/constraint-message-spec.yaml")

    override protected def initQuerease = FieldOrdTestQuerease
  }

  it should "preserve field ordering when jsonizing" in {
    val m = (1 to 5).map(_.toString).map(x => (x, x)).toMap

    val m1: Map[String, Any] = new TreeMap()(Ordering.String) ++ m
    ResultEncoder.encodeAnyToJsonString(m1) shouldBe """{"1":"1","2":"2","3":"3","4":"4","5":"5"}"""

    val m2: Map[String, Any] = new TreeMap()(Ordering.String.reverse) ++ m
    ResultEncoder.encodeAnyToJsonString(m2) shouldBe """{"5":"5","4":"4","3":"3","2":"2","1":"1"}"""
  }
}
