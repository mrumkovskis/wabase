package org.wabase

import org.mojoz.metadata.in.YamlMd
import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers
import spray.json._

import scala.collection.immutable.TreeMap
import scala.math.Ordering

class FieldOrderingSpecs extends FlatSpec with Matchers {
  behavior of "FieldOrderingSpecs"

  object TestApp extends AppBase[TestUsr] with NoAudit[TestUsr] with NoAuthorization[TestUsr]
      with NoValidation with DbAccess with PostgreSqlConstraintMessage {
    object FieldOrdTestQuerease extends TestQuerease("/constraint-message-spec.yaml")

    override protected def initQuerease = FieldOrdTestQuerease
  }

  it should "preserve field ordering when jsonizing" in {
    import TestApp.qio.MapJsonFormat
    val m = (1 to 5).map(_.toString).map(x => (x, x)).toMap

    val m1: Map[String, Any] = new TreeMap()(Ordering.String) ++ m
    m1.toJson.toString shouldBe """{"1":"1","2":"2","3":"3","4":"4","5":"5"}"""

    val m2: Map[String, Any] = new TreeMap()(Ordering.String.reverse) ++ m
    m2.toJson.toString shouldBe """{"5":"5","4":"4","3":"3","2":"2","1":"1"}"""
  }
}
