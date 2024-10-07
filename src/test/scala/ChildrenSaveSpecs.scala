package org.wabase

import akka.actor.ActorSystem
import org.mojoz.metadata.Type
import org.mojoz.querease.FilterType._
import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers
import org.tresql._
import org.wabase.AppBase.{FilterLabel, FilterParameter}

import scala.concurrent.ExecutionContextExecutor


object ChildrenSaveSpecsDtos {
  class MotherWithChildren extends DtoWithId {
    var id: java.lang.Long = null
    var name: String = null
    var sex: String = null
    var daughters: List[MotherWithChildrenDaughters] = Nil
    var sons: List[MotherWithChildrenSons] = Nil
  }
  class MotherWithChildrenDaughters extends Dto {
    var name: String = null
    var sex: String = null
  }
  class MotherWithChildrenSons extends Dto {
    var name: String = null
    var sex: String = null
  }
  val viewNameToClass = Map[String, Class[_ <: Dto]](
    "mother_with_children"            -> classOf[MotherWithChildren],
    "mother_with_children_daughters"  -> classOf[MotherWithChildrenDaughters],
    "mother_with_children_sons"       -> classOf[MotherWithChildrenSons],
  )
}

class ChildrenSaveSpecs extends FlatSpec with Matchers with TestQuereaseInitializer {

  import AppMetadata._

  implicit protected var tresqlResources: Resources = _

  override def beforeAll(): Unit = {
    querease = new TestQuerease("/children-save-specs-metadata.yaml") {
      override lazy val viewNameToClassMap = ChildrenSaveSpecsDtos.viewNameToClass
    }
    qio = new AppQuereaseIo[Dto](querease)
    super.beforeAll()
    tresqlResources = tresqlThreadLocalResources.withConn(tresqlThreadLocalResources.conn)
  }

  import ChildrenSaveSpecsDtos._
  // implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  behavior of "ChildrenSave"

  it should "create persistence metadata" in {
    import org.tresql.OrtMetadata._
    querease.persistenceMetadata("mother_with_children") shouldBe View(
      List(SaveTo("person", Set(), List())), Some(Filters(None, None, None)), "m", List(
        Property("id", KeyValue("if_defined_or_else(:'old key'.id?, :'old key'.id?, :id)", AutoValue(":id"), Some(AutoValue(":id"))), false, true, false),
        Property("name", TresqlValue(":name"), false, true, true),
        Property("sex", TresqlValue("('F')"), false, true, true),
        Property("daughters", ViewValue(View(
          List(SaveTo("person", Set("mother_id"), List())), Some(Filters(Some("(d.sex = 'F')"), Some("(d.sex = 'F')"), Some("(d.sex = 'F')"))), "d", List(
            Property("name", TresqlValue(":name"), false, true, true),
            Property("sex", TresqlValue("('F')"), false, true, true)
          ), null
        ), SaveOptions(true, false, true)), false, true, true),
        Property("sons", ViewValue(View(
          List(SaveTo("person", Set("mother_id"), List())), Some(Filters(Some("(s.sex = 'M')"), Some("(s.sex = 'M')"), Some("(s.sex = 'M')"))), "s", List(
            Property("name", TresqlValue(":name"), false, true, true),
            Property("sex", TresqlValue("('M')"), false, true, true)
          ), null), SaveOptions(true, false, true)), false, true, true
        )
      ), null
    )
  }

  it should "save multiple sets of children to the same table" in {
    val m = new MotherWithChildren
    var m_saved: MotherWithChildren = null
    val qe = querease

    m.name = "Mommy"
    m.id   = qe.save(m)

    m_saved = qe.get[MotherWithChildren](m.id).get
    m_saved.name shouldBe "Mommy"
    m_saved.sex shouldBe "F"
    m_saved.daughters shouldBe Nil
    m_saved.sons      shouldBe Nil

    def test(hadDaughters: Boolean, hadSons: Boolean, hasDaughters: Boolean, hasSons: Boolean) = {
      val was_d1 = new MotherWithChildrenDaughters()
      was_d1.name = "Was-D1"
      val was_d2 = new MotherWithChildrenDaughters()
      was_d2.name = "Was-D2"
      val was_s1 = new MotherWithChildrenSons()
      was_s1.name = "Was-S1"
      val was_s2 = new MotherWithChildrenSons()
      was_s2.name = "Was-S2"

      val is_d1 = new MotherWithChildrenDaughters()
      is_d1.name = "Is-D1"
      val is_d2 = new MotherWithChildrenDaughters()
      is_d2.name = "Is-D2"
      val is_s1 = new MotherWithChildrenSons()
      is_s1.name = "Is-S1"
      val is_s2 = new MotherWithChildrenSons()
      is_s2.name = "Is-S2"

      m.daughters = if (hadDaughters) List(was_d1, was_d2) else Nil
      m.sons      = if (hadSons)      List(was_s1, was_s2) else Nil
      qe.save(m)

      m_saved = qe.get[MotherWithChildren](m.id).get
      if (hadDaughters) {
        m_saved.daughters.length shouldBe 2
        m_saved.daughters(0).name shouldBe was_d1.name
        m_saved.daughters(0).sex  shouldBe "F"
        m_saved.daughters(1).name shouldBe was_d2.name
        m_saved.daughters(1).sex  shouldBe "F"
      } else {
        m_saved.daughters.length shouldBe 0
      }
      if (hadSons) {
        m_saved.sons.length shouldBe 2
        m_saved.sons(0).name shouldBe was_s1.name
        m_saved.sons(0).sex  shouldBe "M"
        m_saved.sons(1).name shouldBe was_s2.name
        m_saved.sons(1).sex  shouldBe "M"
      } else {
        m_saved.sons.length shouldBe 0
      }

      m.daughters = if (hasDaughters) List(is_d1, is_d2) else Nil
      m.sons      = if (hasSons)      List(is_s1, is_s2) else Nil
      qe.save(m)

      m_saved = qe.get[MotherWithChildren](m.id).get
      if (hasDaughters) {
        m_saved.daughters.length shouldBe 2
        m_saved.daughters(0).name shouldBe is_d1.name
        m_saved.daughters(0).sex  shouldBe "F"
        m_saved.daughters(1).name shouldBe is_d2.name
        m_saved.daughters(1).sex  shouldBe "F"
      } else {
        m_saved.daughters.length shouldBe 0
      }
      if (hasSons) {
        m_saved.sons.length shouldBe 2
        m_saved.sons(0).name shouldBe is_s1.name
        m_saved.sons(0).sex  shouldBe "M"
        m_saved.sons(1).name shouldBe is_s2.name
        m_saved.sons(1).sex  shouldBe "M"
      } else {
        m_saved.sons.length shouldBe 0
      }
    }

    for {
      hadDaughters  <- Seq(true, false)
      hadSons       <- Seq(true, false)
      hasDaughters  <- Seq(true, false)
      hasSons       <- Seq(true, false)
    } yield { test(hadDaughters, hadSons, hasDaughters, hasSons) }

    qe.delete(m_saved)
    qe.get[MotherWithChildren](m_saved.id).orNull shouldBe null
  }
}
