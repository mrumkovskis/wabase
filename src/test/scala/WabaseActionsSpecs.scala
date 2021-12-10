package org.wabase

import org.mojoz.querease.{ValidationException, ValidationResult}
import org.scalatest.flatspec.{AsyncFlatSpec, AsyncFlatSpecLike}
import org.tresql.ThreadLocalResources
import org.wabase.QuereaseActionsDtos.PersonWithHealthDataHealth

object WabaseActionDtos {
  class Purchase extends Dto {
    var customer: String = null
    var purchase_time: java.sql.Timestamp = null
    var item: String = null
    var amount: BigDecimal = null
  }
  class PurchaseWithId extends Purchase with DtoWithId {
    var id: java.lang.Long = null
  }
  class PersonWithHealthAndShop extends Dto {
    var name: String = null
    var sex: String = null
    var birthdate: java.sql.Date = null
    var health: List[PersonWithHealthDataHealth] = Nil
    var purchases: List[Purchase] = Nil
  }

  val viewNameToClass = Map[String, Class[_ <: Dto]](
    "purchase" -> classOf[PurchaseWithId],
    "person_health_and_shop" -> classOf[PersonWithHealthAndShop],
    "person_health_and_shop_health" -> classOf[PersonWithHealthDataHealth],
    "person_health_and_shop_purchases" -> classOf[Purchase]
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
      override implicit val tresqlResources: ThreadLocalResources = WabaseActionsSpecs.this.tresqlThreadLocalResources
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

  protected def doAction[T](action: String,
                         view: String,
                         params: Map[String, Any],
                         deferredTransformer: Iterator[T] => QuereaseResult = null) = {
    app.doWabaseAction(action, view, params)
      .run
      .map(_._2)
      .map {
        case dr: DeferredQuereaseResult[T@unchecked] =>
          Option(deferredTransformer)
            .map(dr.flatMap)
            .getOrElse(dr.flatMap(r => ListResult(r.toList)))
        case r => r
      }
  }

  protected def iteratorResultTransformer(i: Iterator[AppQuerease#DTO]) =
    ListResult(i.map(_.toMap(app.qe)).map(removeIds).toList)

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
      doAction("save", "purchase", purchase)
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
      doAction("save", "person", person).flatMap { _ =>
        doAction("save", "person_health", vaccine).flatMap { _ =>
          doAction("save", "purchase", purchase)
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

    doAction("save", "payment", payment).flatMap { _ =>
      doAction("save", "purchase", purchase)
    }.flatMap { _ =>
      doAction("list",
        "purchase",
        Map("sort" -> "id"),
        iteratorResultTransformer
      ).map {
        case ListResult(lr)  =>
          lr should be (
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

  it should "retrieve person health and purchase data" in {
    val person = Map(
      "name" -> "Mario",
      "sex" -> "M",
      "birthdate" -> "1988-09-20",
      "accounts" -> List(Map(
        "number" -> "MMMM",
        "balance" -> 0,
        "last_modified" -> java.sql.Timestamp.valueOf("2021-12-8 12:14:10.0")
      ))
    )
    val vaccine = Map(
      "name" -> "Mr. Mario",
      "vaccine" -> "BioNTech",
      "manipulation_date" -> java.sql.Date.valueOf("2021-08-15")
    )
    val payment = Map(
      "beneficiary" -> "MMMM",
      "originator" -> null,
      "amount" -> 20
    )
    val purchase = Map(
      "customer" -> "Mr. Mario",
      "purchase_time" -> java.sql.Timestamp.valueOf("2021-12-08 12:15:33"),
      "item" -> "beer",
      "amount" -> 2
    )
    doAction("save", "person", person).flatMap { _ =>
      doAction("save", "person_health", vaccine).flatMap { _ =>
        doAction("save", "payment", payment).flatMap { _ =>
          doAction("save", "purchase", purchase)
        }
      }
    }.flatMap { _ =>
      doAction("list",
        "person_health_and_shop",
        Map(),
        iteratorResultTransformer
      ).map {
        case ListResult(lr)  =>
          lr should be (
            List(
              Map(
                "health" ->
                  List(Map(
                    "manipulation_date" -> java.sql.Date.valueOf("2021-06-05"),
                    "vaccine" -> "AstraZeneca",
                    "had_virus" -> null
                  )),
                "name" -> "Mr. Gunza",
                "purchases" ->
                  List(Map(
                    "customer" -> "Mr. Gunza",
                    "purchase_time" -> java.sql.Timestamp.valueOf("2021-12-04 15:15:23.0"),
                    "item" -> "joystick",
                    "amount" -> 60.00
                  )),
                "sex" -> "M",
                "birthdate" -> java.sql.Date.valueOf("1999-04-23")
              ), Map(
                "health" ->
                  List(Map(
                    "manipulation_date" -> java.sql.Date.valueOf("2021-08-15"),
                    "vaccine" -> "BioNTech",
                    "had_virus" -> null
                  )),
                "name" -> "Mr. Mario",
                "purchases" ->
                  List(Map(
                    "customer" -> "Mr. Mario",
                    "purchase_time" -> java.sql.Timestamp.valueOf("2021-12-08 12:15:33.0"),
                    "item" -> "beer",
                    "amount" -> 2.00
                  )),
                "sex" -> "M",
                "birthdate" -> java.sql.Date.valueOf("1988-09-20")
              )
            )
        )
        case x => fail(s"Invalid purchase list: $x")
      }
    }
  }
}
