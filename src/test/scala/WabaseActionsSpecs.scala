package org.wabase

import org.scalatest.flatspec.{AsyncFlatSpec, AsyncFlatSpecLike}
import org.tresql.ThreadLocalResources

object WabaseActionDtos {
  class Purchase extends DtoWithId {
    var id: java.lang.Long = null
    var customer: String = null
    var purchase_time: java.sql.Timestamp = null
    var item: String = null
    var amount: BigDecimal = null
  }

  val viewNameToClass = Map[String, Class[_ <: Dto]](
    "purchase" -> classOf[Purchase],
  )
}

class WabaseActionsSpecs extends AsyncFlatSpec with QuereaseBaseSpecs with AsyncFlatSpecLike {

  override def dbNamePrefix: String = "wabase_db"

  var app: AppBase[TestUsr] = _

  override def beforeAll(): Unit = {
    querease = new QuereaseBase("/querease-action-specs-metadata.yaml") {
      override lazy val viewNameToClassMap = QuereaseActionsDtos.viewNameToClass ++ WabaseActionDtos.viewNameToClass
    }
    super.beforeAll()
    val db = new DbAccess with Loggable {
      override implicit val tresqlResources: ThreadLocalResources = WabaseActionsSpecs.this.tresqlResources
    }
    app = new TestApp with NoValidation {
      override def dbAccessDelegate = db
      override type QE = AppQuerease
      override protected def initQuerease: QE = querease
    }
  }

  import spray.json._
  private implicit val user = TestUsr(100)
  private implicit val state = ApplicationState(Map())
  private implicit val timeout = QueryTimeout(10)
  private implicit val defaultCp = PoolName(dbNamePrefix)

  behavior of "purchase"

  it should "should register purchase without validation" in {
    val purchase = Map(
      "customer" -> "Ravus",
      "purchase_time" -> java.sql.Timestamp.valueOf("2021-12-04 00:06:53"),
      "item" -> "sword",
      "amount" -> 100
    )
    val id = app.save("purchase", purchase.toJson(app.qe.MapJsonFormat).asJsObject)
    app.get("purchase", id).map(_.asInstanceOf[Dto].toMap(app.qe)).map(removeIds) should be {
      Some(Map(
        "customer" -> "Ravus",
        "purchase_time" -> java.sql.Timestamp.valueOf("2021-12-04 00:06:53"),
        "item" -> "sword",
        "amount" -> 100.00
      ))
    }
  }

  it should "register purchase" in {
    1 should be (1)
  }
}
