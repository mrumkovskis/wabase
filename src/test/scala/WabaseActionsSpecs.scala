package org.wabase

import org.mojoz.querease.{ValidationException, ValidationResult}
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
      override val DefaultCp: PoolName = PoolName("wabase_db")
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

  it should "should purchase without validation" in {
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

  it should "fail purchase due to antivax error" in {
    val purchase = Map(
      "customer" -> "Ravus",
      "purchase_time" -> java.sql.Timestamp.valueOf("2021-12-04 14:49:05"),
      "item" -> "hat",
      "amount" -> 5
    )
    recoverToExceptionIf[ValidationException](
      app.doWabaseAction("save", "purchase", purchase)
    ).map(_.details should be(List(ValidationResult(Nil, List("Pardon, customer 'Ravus' is not vaccinated...")))))
  }

  it should "fail purchase due to insufficient funds" in {
    val person = Map(
      "name" -> "Gunza",
      "sex" -> "M",
      "birthdate" -> "1999-04-23",
      "accounts" -> List(Map(
        "number" -> "GGGG",
        "balance" -> 0,
        "last_modified" -> java.sql.Timestamp.valueOf("2021-12-7 15:24:01.0")
      ))
    )
    val vaccine = Map(
      "name" -> "Mr. Gunza",
      "vaccine" -> "AstraZeneca",
      "manipulation_date" -> java.sql.Date.valueOf("2021-06-05")
    )
    val purchase = Map(
      "customer" -> "Mr. Gunza",
      "purchase_time" -> java.sql.Timestamp.valueOf("2021-12-04 15:15:23"),
      "item" -> "joystick",
      "amount" -> 60
    )
    recoverToExceptionIf[ValidationException](
      app.doWabaseAction("save", "person", person).flatMap { _ =>
        app.doWabaseAction("save", "person_health", vaccine).flatMap { _ =>
          app.doWabaseAction("save", "purchase", purchase)
        }
      }
    ).map(_.details should be(List(ValidationResult(Nil, List("Insufficient funds, available (0.00)")))))
  }

  it should "make purchase" in {
    val payment = Map(
      "beneficiary" -> "GGGG",
      "originator" -> null,
      "amount" -> 100
    )
    val purchase = Map(
      "customer" -> "Mr. Gunza",
      "purchase_time" -> java.sql.Timestamp.valueOf("2021-12-04 15:15:23"),
      "item" -> "joystick",
      "amount" -> 60
    )

    app.doWabaseAction("save", "payment", payment).flatMap { _ =>
      app.doWabaseAction("save", "purchase", purchase)
    }.flatMap { _ =>
      app.doWabaseAction("list", "purchase", Map("sort" -> "id")).flatMap {
        case IteratorResult(i: Iterator[AppQuerease#DTO])  =>
          i.map(_.toMap(app.qe)).map(removeIds).toList should be (
            List(
              Map(
                "customer" -> "Ravus",
                "purchase_time" -> java.sql.Timestamp.valueOf("2021-12-04 00:06:53.0"),
                "item" -> "sword",
                "amount" -> 100.00
              ), Map(
                "customer" -> "Mr. Gunza",
                "purchase_time" -> java.sql.Timestamp.valueOf("2021-12-04 15:15:23.0"),
                "item" -> "joystick",
                "amount" -> 60.00
              )
            )
          )
        case x => fail(s"Invalid purchase list: $x")
      }
    }
  }
}
