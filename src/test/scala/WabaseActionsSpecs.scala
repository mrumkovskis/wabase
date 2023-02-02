package org.wabase

import akka.actor.ActorSystem
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, MessageEntity, StatusCodes}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.util.ByteString
import org.mojoz.querease.{ValidationException, ValidationResult}
import org.scalatest.flatspec.{AsyncFlatSpec, AsyncFlatSpecLike}
import org.scalatest.matchers.should.Matchers
import org.tresql.{MissingBindVariableException, Query, ThreadLocalResources}
import org.wabase.QuereaseActionsDtos.PersonWithHealthDataHealth

import java.io.File
import java.nio.file.Files
import java.util.UUID
import scala.collection.immutable.{ListMap, Seq}
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.Try

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

  class env_test_1 extends DtoWithId {
    var id: jLong = null
    var name: String = null
    var sex: String = null
    var birthdate: java.sql.Date = null
  }

  class env_test_2 extends env_test_1 {
    var surname: String = null
  }

  class env_test_3 extends env_test_1 {
    var surname: String = null
  }

  class env_test_4 extends env_test_1

  val viewNameToClass = Map[String, Class[_ <: Dto]](
    "env_test_1" -> classOf[env_test_1],
    "env_test_2" -> classOf[env_test_2],
    "env_test_3" -> classOf[env_test_3],
    "env_test_4" -> classOf[env_test_4],
    "purchase" -> classOf[PurchaseWithId],
    "person_health_and_shop" -> classOf[PersonWithHealthAndShop],
    "person_health_and_shop_health" -> classOf[PersonWithHealthDataHealth],
    "person_health_and_shop_purchases" -> classOf[Purchase]
  )
}

class WabaseActionsSpecs extends AsyncFlatSpec with Matchers with TestQuereaseInitializer with AsyncFlatSpecLike {

  override def dbNamePrefix: String = "wabase_db"

  class WabaseActionsService(as: ActorSystem) extends TestAppServiceNoDeferred(as) {
    val route = crudAction(user)
  }

  var app: TestApp = _
  var marshallers: AppProvider[TestUsr] with QuereaseMarshalling with Execution = _
  var service: WabaseActionsService = _

  override def beforeAll(): Unit = {
    querease = new TestQuerease("/querease-action-specs-metadata.yaml") {
      override lazy val viewNameToClassMap = QuereaseActionsDtos.viewNameToClass ++ WabaseActionDtos.viewNameToClass

      override def doHttpRequest(reqF: Future[HttpRequest])(implicit as: ActorSystem): Future[HttpResponse] =
        reqF.flatMap { req =>
          Route.toFunction(service.route)(as)(req)
        }
    }
    super.beforeAll()
    val db = new DbAccess with Loggable {
      override implicit val tresqlResources: ThreadLocalResources = WabaseActionsSpecs.this.tresqlThreadLocalResources
    }
    app = new TestApp with NoValidation {
      override val DefaultCp: PoolName = PoolName("wabase_db")
      override protected val fileStreamerConnectionPool: PoolName = DefaultCp
      override def dbAccessDelegate = db
      override protected def initQuerease: QE = querease
      override protected def shouldAddResultToContext(context: AppActionContext): Boolean =
        Set("result_audit_test") contains context.viewName

      override protected def afterWabaseAction(context: AppActionContext, result: Try[QuereaseResult]): Unit =
        if (context.viewName == "result_audit_test") {
          val res = context.serializedResult
          implicit val as = marshallers.system
          res
            .via(marshallers.serializedResultToJsonFlow(false,
              new ResultRenderer.ViewFieldFilter(context.viewName, qe.nameToViewDef)))
            .runFold(ByteString.empty){_ ++ _}
            .map { bytes =>
              val id = context.values("id").toString.toLong + 1
              db.transaction(template = tresqlResources.resourcesTemplate, poolName = DefaultCp) { r =>
                Query("+simple_table {id = ?, value = ?}", id, bytes.decodeString("UTF-8"))(r)
              }
            }(scala.concurrent.ExecutionContext.global) // do not use AsyncFlatSpec context so that no blocking occurs
        }

      override lazy val rootPath =
        new File(System.getProperty("java.io.tmpdir"), "wabase-actions-specs/" + UUID.randomUUID().toString).getPath
    }
    val myApp = app
    marshallers =
      new ExecutionImpl()(ActorSystem("actions-spec-system"))
        with Execution with AppProvider[TestUsr] with QuereaseMarshalling with OptionMarshalling {
        override type App = AppBase[TestUsr]
        override protected def initApp: App = myApp
      }

    service = new WabaseActionsService(as) {
      override def initApp = myApp
    }
  }

  override def afterAll(): Unit = {
    val p = new File(app.rootPath).toPath
    if (p.toFile.exists())
      Files
        .walk(p)
        .sorted(java.util.Comparator.reverseOrder())
        .map[java.io.File](_.toFile)
        .forEach(_.delete)
    super.afterAll()
  }

  import spray.json._
  private implicit val user = TestUsr(100)
  private implicit val timeout = QueryTimeout(10)
  private implicit val defaultCp = PoolName(dbNamePrefix)
  private implicit val as = ActorSystem("wabase-action-specs")

  protected def doAction[T](action: String,
                            view: String,
                            values: Map[String, Any],
                            env: Map[String, Any] = Map.empty,
                            removeIdsFlag: Boolean = true,
                            keyValues: Seq[Any] = Nil,
                          ) = {
    implicit val state = ApplicationState(env)
    implicit val fileStreamer: AppFileStreamer[TestUsr] = app
    implicit val req: HttpRequest = null
    app.doWabaseAction(action, view, keyValues, Map.empty, values)
      .map(_.result)
      .flatMap {
        case sr: QuereaseSerializedResult =>
          implicit val marshaller     = marshallers.toEntityQuereaseSerializedResultMarshaller(view,
            new ResultRenderer.ViewFieldFilter(view, app.qe.nameToViewDef))
          implicit val unmarshaller_1 = marshallers.toMapUnmarshallerForView(view)
          implicit val unmarshaller_2 = marshallers.toSeqOfMapsUnmarshallerForView(view)
          Marshal(sr).to[MessageEntity].flatMap { entity =>
            if  (sr.isCollection)
                 Unmarshal(entity).to[Seq[Map[String, Any]]]
            else Unmarshal(entity).to[Map[String, Any]]
          }
            .map(r => if (removeIdsFlag) removeIds(r) else r)
        case r => Future.successful(r)
      }
  }

  protected def unmarshalResponse(resp: HttpResponse): Future[Any] = {
    if (resp.status.isRedirection()) Future.successful {
      resp.headers.find(_.is("location")).map(_.value()).getOrElse("")
    } else resp.entity.toStrict(1.second)
      .map(_.data)
      .map { d =>
        Try(new CborOrJsonAnyValueDecoder().decode(d))
          .toOption
          .getOrElse(d.decodeString("UTF-8"))
      }
  }

  private implicit val state = ApplicationState(Map())

  behavior of "purchase"

  // this is deprecated use wabase action calls
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
        doAction("save", "person_health", vaccine).flatMap { r =>
          r shouldBe StatusResult(303, RedirectStatus(TresqlUri.Uri("person_health", List("Mr. Gunza", "2021-06-05"), ListMap("par1" -> "val1", "par2" -> "val2"))))
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
      ).map { _ should be ( YamlUtils.parseYamlData(
        """
        - customer: Ravus
          purchase_time: 2021-12-04 00:06:53.0
          item: sword
          amount: 100.00
        - customer: Mr. Gunza
          purchase_time: 2021-12-04 15:15:23.0
          item: joystick
          amount: 60.00
        """
      ))}
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
      doAction("save", "person_health", vaccine).flatMap { r =>
        r shouldBe StatusResult(303, RedirectStatus(TresqlUri.Uri("person_health", List("Mr. Mario", "2021-08-15"), ListMap("par1" -> "val1", "par2" -> "val2"))))
        doAction("save", "payment", payment).flatMap { _ =>
          doAction("save", "purchase", purchase)
        }
      }
    }.flatMap { _ =>
      doAction("list",
        "person_health_and_shop",
        Map("names" -> List("Mr. Gunza", "Mr. Mario")),
      ).map { _ should be ( YamlUtils.parseYamlData(
        """
        - name: Mr. Gunza
          sex: M
          birthdate: 1999-04-23
          health:
            - manipulation_date: 2021-06-05
              vaccine: AstraZeneca
              had_virus: null
          purchases:
            - customer: Mr. Gunza
              purchase_time: 2021-12-04 15:15:23.0
              item: joystick
              amount: 60.00
        - name: Mr. Mario
          sex: M
          birthdate: 1988-09-20
          health:
            - manipulation_date: 2021-08-15
              vaccine: BioNTech
              had_virus: null
          purchases:
            - customer: Mr. Mario
              purchase_time: 2021-12-08 12:15:33.0
              item: beer
              amount: 2.00
        """
      ))}
    }
  }

  it should "throw MissingBindVariableException on list" in {
    recoverToSucceededIf[MissingBindVariableException] {
      doAction("list",
        "person_health_and_shop", Map())
    }
  }

  // this is deprecated, use wabase actions instead
  it should "retrieve person health and purchase data old style" in {
    //can recursive map transformation remove elements?
    import MapRecursiveExtensions._
    def tf: PartialFunction[(Any, Any), Any] = {
      case ("name", v) => v
      case ("name" / "health" / "vaccine", v) => v
      case ("name" / "purchases" / "item", v) => v
      case x => null
    }
    def reduceMap(m: Map[String, _], struct: List[_]): Map[String, _] = {
      struct.map {
        case (name: String, chStruct: List[_]) if m.contains(name) => name -> (m(name) match {
          case l: List[Map[String, _]@unchecked] => l.map(e => reduceMap(e, chStruct))
          case chm: Map[String@unchecked, _] => reduceMap(chm, chStruct)
          case x => x
        })
        case name: String if m.contains(name) => name -> m(name)
      }
    }.toMap
    app.list("person_health_and_shop", Map("names" -> List("Mr. Gunza", "Mr. Mario")))
      .map(_.toMap(app.qe)).toList
      //.map(_ recursiveMap tf)
      .map(reduceMap(_, List("name", "purchases" -> List("item"), "health" -> List("vaccine")))) should be(
      List(
        Map(
          "name" -> "Mr. Gunza",
          "purchases" -> List(Map("item" -> "joystick")),
          "health" -> List(Map("vaccine" -> "AstraZeneca"))),
        Map(
          "name" -> "Mr. Mario",
          "purchases" -> List(Map("item" -> "beer")),
          "health" -> List(Map("vaccine" -> "BioNTech"))
        )
      )
    )
  }

  it should "throw MissingBindVariableException on old style list" in {
    assertThrows[MissingBindVariableException] {
      app.list("person_health_and_shop", Map()).toList
    }
  }

  it should "get purchase from tresql" in {
    doAction("get", "purchase_get",
      Map("purchase_time" -> "2021-12-04 15:15:23.0", "customer" -> "Mr. Gunza"))
      .map {
        _ should be (Map("customer" -> null, "purchase_time" -> null, "item" -> "joystick", "amount" -> 60.0))
      }
  }

  it should "delete purchase" in {
    doAction("delete", "purchase",
      Map("purchase_time" -> "2021-12-08 12:15:33.0", "customer" -> "Mr. Mario"))
      .map(_ should be(QuereaseDeleteResult(1)))
  }

  it should "delete purchase old style" in {
    doAction("get", "purchase",
      Map("purchase_time" -> "2021-12-04 15:15:23.0", "customer" -> "Mr. Gunza"), removeIdsFlag = false)
      .map {
        case purch: Map[String@unchecked, _] =>
          app.delete("purchase", purch("id").toString.toLong) should be(1)
        case x => fail(s"Unexpected result: $x")
      }
  }

  it should "count" in {
    doAction("count", "purchase", Map.empty)
      .map(_ shouldBe LongResult(1))
  }

  it should "manage env properly" in {
    val person = Map(
      "birthdate" -> "1988-09-20",
    )
    val poorEnv = Map(
      "current_person_name"    -> "EnvTestName",
      "current_person_surname" -> "EnvTestSurname",
    )
    val updateDisabledEnv = poorEnv ++ Map("update_enabled" -> false)
    val updateOkEnv       = poorEnv ++ Map("update_enabled" -> true)

    for {
      t1 <-
        recoverToExceptionIf[MissingBindVariableException](
          doAction("insert", "env_test_2", person, poorEnv)
        ).map(_.getMessage shouldBe "Missing bind variable: update_enabled")
      t2 <-
        recoverToExceptionIf[org.mojoz.querease.NotFoundException](
          doAction("insert", "env_test_2", person, updateDisabledEnv)
        ).map(_.getMessage shouldBe "Record not updated in table(s): person")
      t3 <-
        doAction("insert", "env_test_2", person, updateOkEnv).flatMap { r =>
          val id = r match { case kr: KeyResult => kr.ir.id case _ => -1 }
          implicit val qe = querease
          doAction("get", "env_test_2", Map("id" -> id), removeIdsFlag = false).map {
            case map: Map[_, _] => map shouldBe Map(
              "id" -> id,
              "name" -> "EnvTestName",
              "surname" -> "EnvTestSurname",
              "sex" -> "M",
              "birthdate" -> new java.sql.Date(Format.parseDate("1988-09-20").getTime),
            )
          }
        }
      t4 <-
        doAction("insert", "env_test_3", person, updateOkEnv).flatMap { r =>
          val id = r match { case kr: KeyResult => kr.ir.id case _ => -1 }
          implicit val qe = querease
          doAction("get", "env_test_3", Map("id" -> id), removeIdsFlag = false).map {
            case map: Map[_, _] => map shouldBe Map(
              "id" -> id,
              "name" -> "EnvTestName",
              "surname" -> "EnvTestSurname",
              "sex" -> "M",
              "birthdate" -> new java.sql.Date(Format.parseDate("1988-09-20").getTime),
            )
          }
        }
      t5 <-
        doAction("insert", "env_test_4", person ++ Map("sex" -> "M"), updateOkEnv).flatMap { r =>
          val id = r match { case kr: KeyResult => kr.ir.id case _ => -1 }
          implicit val qe = querease
          doAction("update", "env_test_4", person ++ Map("id" -> id, "sex" -> "F"),
                    updateOkEnv, removeIdsFlag = false, keyValues = Seq(id)).flatMap { r =>
            doAction("get", "env_test_4", Map("id" -> id), removeIdsFlag = false).map {
              case map: Map[_, _] => map shouldBe Map(
                "id" -> id,
                "name" -> "Mika",
                "sex" -> "F",
                "birthdate" -> new java.sql.Date(Format.parseDate("1988-09-20").getTime),
              )
            }
          }
        }
    } yield  {
      t5
    }
  }

  it should "return status" in {
    for {
      t1 <-
        doAction("get", "status_test_1", Map()).map {
          _ shouldBe StatusResult(200, null)
        }
      t2 <-
        doAction("save", "status_test_1", Map("status" -> "ok")).map {
          _ shouldBe StatusResult(200, StringStatus("ok"))
        }
      t3 <-
        doAction("count", "status_test_1", Map("status" -> "ok")).map {
          _ shouldBe StatusResult(200, null)
        }
      t4 <-
        doAction("list", "status_test_1", Map("status" -> "redirect")).map {
          _ shouldBe StatusResult(303, RedirectStatus(TresqlUri.Uri("/data", List("path", "redirect"), ListMap())))
        }
      t5 <-
        doAction("get", "status_test_2", Map("id" -> 1)).map {
          _ shouldBe StatusResult(303, RedirectStatus(TresqlUri.Uri("data/path", List("1"), ListMap())))
        }
      t6 <-
        doAction("save", "status_test_2", Map("id" -> 1)).map {
          _ shouldBe StatusResult(303, RedirectStatus(TresqlUri.Uri("data/path", Nil, ListMap("id" -> "1"))))
        }
      t7 <-
        doAction("count", "status_test_2", Map()).map {
          _ shouldBe StatusResult(303, RedirectStatus(TresqlUri.Uri("data/path", Nil, ListMap())))
        }
      t8 <-
        doAction("list", "status_test_2", Map()).map {
          _ shouldBe StatusResult(303, RedirectStatus(TresqlUri.Uri("data/path", Nil, ListMap())))
        }
      t9 <-
        doAction("save", "status_test_3", Map()).map {
          _ shouldBe StatusResult(303, RedirectStatus(TresqlUri.Uri("303", List(), ListMap())))
        }
      t10 <-
        doAction("get", "status_test_3", Map("id" -> 2)).map {
          _ shouldBe StatusResult(303, RedirectStatus(TresqlUri.Uri("data/path/2", List(), ListMap())))
        }
      t11 <-
        doAction("list", "status_test_3", Map("id" -> 3)).map {
          _ shouldBe StatusResult(303, RedirectStatus(TresqlUri.Uri("data/path", List("3"), ListMap("par1" -> "val-of-par1"))))
        }
      t12 <-
        doAction("count", "status_test_3", Map("id" -> 4)).map {
          _ shouldBe StatusResult(303, RedirectStatus(TresqlUri.Uri(null, List("4"), ListMap("par1" -> "5"))))
        }
      t13 <-
        doAction("save", "status_test_4", Map("id" -> null)).map {
          _ shouldBe StatusResult(303, RedirectStatus(TresqlUri.Uri("data/path", List(null), ListMap())))
        }
      t14 <-
        doAction("get", "status_test_4", Map("id" -> null)).map {
          _ shouldBe StatusResult(303, RedirectStatus(TresqlUri.Uri("data/path", List(), ListMap("id" -> null))))
        }
      t15 <-
        doAction("list", "status_test_4", Map("id" -> null)).map {
          _ shouldBe StatusResult(303, RedirectStatus(TresqlUri.Uri(null, List(), ListMap())))
        }
    } yield {
      t15
    }
  }

  it should "do invocations" in {
    for {
      t1 <- doAction("get", "invocation_test_1", Map()).map {
        _ shouldBe StatusResult(200, StringStatus("val1 val2"))
      }
      t2 <- doAction("save", "invocation_test_1", Map()).map {
        _ shouldBe MapResult(Map("nr" -> 2.5))
      }
    } yield {
      t2
    }
  }

  it should "execute actions" in {
    for {
      t1 <- doAction("insert", "insert_update_test_1", Map("id" -> 42)).map {
        _ shouldBe Map("value" -> "INS")
      }
      t2 <- doAction("update", "insert_update_test_1", Map("id" -> 42)).map {
        _ shouldBe Map("value" -> "UPD")
      }
    } yield {
      t2
    }
  }

  it should "evaluate if" in {
    for {
      t1 <- doAction("save", "if_test_1", Map("value" -> "yes")).map {
        _ shouldBe Map("code" -> "if_test_1", "parent" -> null, "value" -> "yes_value")
      }
      t2 <- doAction("save", "if_test_1", Map("value" -> "no")).map {
        _ shouldBe Map("code" -> "if_test_1", "parent" -> null, "value" -> "no_value")
      }
      t2 <- doAction("save", "if_test_1", Map("value" -> "x")).map {
        _ shouldBe Map("code" -> "if_test_1", "parent" -> null, "value" -> "no_value")
      }
      t3 <- doAction("get", "if_test_2", Map("value" -> true)).map {
        _ shouldBe StatusResult(200, StringStatus("yes"))
      }
      t4 <- doAction("get", "if_test_2", Map("value" -> false)).map {
        _ shouldBe StatusResult(200, StringStatus(null))
      }
      t5 <- doAction("list", "if_test_2", Map("value" -> true)).map {
        _ shouldBe StatusResult(200, StringStatus("yes"))
      }
      t6 <- doAction("list", "if_test_2", Map("value" -> false)).map {
        _ shouldBe StatusResult(200, StringStatus("init"))
      }
    } yield {
      t4
    }
  }

  it should "evaluate foreach" in {
    for {
      t1 <- doAction("save", "foreach_test_1",
        Map("code" -> "foreach_test_1", "value" -> "top",
          "children" -> List(
            Map("code" -> "foreach_test_1.ch_1", "value" -> "child1"),
            Map("code" -> "foreach_test_1.ch_2", "value" -> "child2"),
          )
        )
      ).map {
        _ shouldBe Map("code" -> "foreach_test_1", "parent" -> null, "value" -> "top", "children" ->
          List(
            Map("code" -> "foreach_test_1.ch_1", "parent" -> "foreach_test_1", "value" -> "child1", "children" -> List()),
            Map("code" -> "foreach_test_1.ch_2", "parent" -> "foreach_test_1", "value" -> "child2", "children" -> List())
          )
        )
      }
      t2 <- doAction("save", "foreach_test_2",
        Map("code" -> "foreach_test_2", "value" -> "old",
          "children" -> List(
            Map("code" -> "foreach_test_2.ch_1", "value" -> "new child"),
          )
        )
      ).map {
        _ shouldBe Map("code" -> "foreach_test_2", "parent" -> null, "value" -> "old", "children" -> List())
      }
      t3 <- doAction("save", "foreach_test_2",
        Map("code" -> "foreach_test_2.1", "value" -> "new",
          "children" -> List(
            Map("code" -> "foreach_test_2.1.ch_1", "value" -> "old child"),
            Map("code" -> "foreach_test_2.1.ch_2", "value" -> "new child"),
          )
        )
      ).map {
        _ shouldBe Map("code" -> "foreach_test_2.1", "parent" -> null, "value" -> "new", "children" ->
          List(
            Map("code" -> "foreach_test_2.1.ch_2", "parent" -> "foreach_test_2.1", "value" -> "new child", "children" -> List())
          )
        )
      }
      t4 <- doAction("update", "foreach_test_1",
        Map("code" -> "foreach_test_1", "value" -> "top_upd",
          "children" -> List(
            Map("code" -> "foreach_test_1.ch_1", "value" -> "child1_upd"),
            Map("code" -> "foreach_test_1.ch_2", "value" -> "child2_upd"),
          )
        )
      ).map {
        _ shouldBe Map("code" -> "foreach_test_1", "parent" -> null, "value" -> "top_upd", "children" ->
          List(
            Map("code" -> "foreach_test_1.ch_1", "parent" -> "foreach_test_1", "value" -> "child1_upd", "children" -> List()),
            Map("code" -> "foreach_test_1.ch_2", "parent" -> "foreach_test_1", "value" -> "child2_upd", "children" -> List())
          )
        )
      }
    } yield {
      t4
    }
  }

  it should "process result source after wabase result" in {
    val id = 55
    doAction("get", "result_audit_test", Map("id" -> id))
      .map { _ =>
        Thread.sleep(200) // wait until hopefully afterWabaseAction method is completed
        app.dbAccess.withConn(template = app.dbAccess.tresqlResources.resourcesTemplate, poolName = app.DefaultCp) { implicit r =>
          val res = Query("simple_table [id = ?] {value}", id + 1).unique[String]
          app.qe.cborOrJsonDecoder
            .decodeToMap(ByteString(res), "result_audit_test")(app.qe.viewNameToMapZero) shouldBe Map("id" -> 55, "value" -> "data")
        }
      }
  }

  it should "remove field from data set" in {
    for {
      t1 <- doAction("insert", "remove_var_test", Map("var1" -> "data1", "var2" -> "data2"))
        .map { _ shouldBe MapResult (Map ("var1" -> "data1")) }
      t2 <- doAction("update", "remove_var_test", Map("var1" -> "data1", "var2" -> "data2"))
        .map { _ shouldBe MapResult(Map("var2" -> "data2")) }
    } yield {
      t2
    }
  }

  it should "do file operations" in {
    def checkFile(fileRes: FileResult) =
      fileRes.fileStreamer.getFileInfo(fileRes.fileInfo.id, fileRes.fileInfo.sha_256)
        .map(fi => fi.source.runWith(AppFileStreamer.sha256sink).map(_ -> fi.sha_256))
        .map { _.map { case (hash1, hash2) => hash1 shouldBe hash2 } }
        .get
    for {
      t1 <- doAction("list", "to_file_test1", Map())
        .mapTo[FileInfoResult]
        .map(_.fileInfo)
        .map(fi => (fi.filename, fi.size))
        .map { _ shouldBe ("persons" -> 254) }
      t2 <- doAction("list", "to_file_test2", Map())
        .mapTo[FileResult]
        .flatMap(checkFile)
      t3 <- doAction("list", "to_file_test3", Map())
        .mapTo[FileResult]
        .flatMap(checkFile)
    } yield {
      t3
    }
  }

  it should "do db use, transaction operations" in {
    for {
      t1 <- doAction("insert", "owner", Map("name" -> "Pedro", "address" -> "Morocco"))
        .flatMap {
          case KeyResult(_, _, key) => doAction("get", "owner", Map(), keyValues = key)
        }.map { _ shouldBe Map("name" -> "Pedro", "address" -> "Morocco") }
      t2 <- recoverToExceptionIf[NullPointerException](doAction("list", "owner", Map()))
        .map(_.getMessage shouldBe "Connection not found in environment." )
    } yield {
      t1
    }
  }

  it should "do http operations" in {
    for {
      t1 <- doAction("get", "http_test_1", Map())
        .mapTo[HttpResult]
        .flatMap(res => unmarshalResponse(res.response))
        .map { _ shouldBe "val1 val2" }
      t2 <- doAction("list", "http_test_2", Map())
        .map { _ shouldBe StatusResult(200, StringStatus("val1 val2")) }
      t3 <- doAction("insert", "http_test_2", Map())
        .map {
          _ shouldBe StatusResult(200, StringStatus("person_health?/Mr.%20Mario/2022-04-11?par1=val1&par2=val2"))
        }
      t4 <- doAction("update", "http_test_2", Map("name" -> "Mr. Gunza",
        "manipulation_date" -> "2022-09-10", "vaccine" -> "Pfizer"))
        .mapTo[HttpResult]
        .flatMap(res => unmarshalResponse(res.response))
        .map { _ shouldBe "person_health?/Mr.%20Gunza/2022-09-10?par1=val1&par2=val2" }
      t5 <- doAction("insert", "forest", Map("nr" -> "OF1", "owner" -> "Pedro",
        "area" -> 1000, "trees" -> "oaks"))
        .map { case KeyResult(_, _, key) => key shouldBe List("OF1") }
      t6 <- doAction("update", "http_forest", Map("area" -> 20.5), keyValues = List("OF1"))
        .mapTo[HttpResult]
        .map { _.response.status shouldBe StatusCodes.SeeOther }
      t7 <- doAction("get", "forest", Map(), keyValues = List("OF1"))
        .map { _ shouldBe Map("nr" -> "OF1", "owner" -> "Pedro", "area" -> 20.5, "trees" -> "oaks") }
    } yield {
      t1
    }
  }

  it should "do json codec operation" in {
    def enc(v: Any) = {
      import ResultEncoder._
      import JsonEncoder._
      new String(encodeJsValue(v), "UTF-8")
    }
    for {
      t1 <- doAction("get", "json_codec_1", Map("value" ->
        enc(Map("trees" -> "pine", "area" -> 23.5, "notes" -> null))))
        .map { _ shouldBe MapResult(Map("trees" -> "pine", "area" -> 23.5, "notes" -> null)) }
      t2 <- doAction("insert", "json_codec_1", Map("value" ->
        enc(Map("trees" -> "pine", "area" -> 23.5, "owner" -> "Pedro", "nr" -> "OF2"))))
        .mapTo[HttpResult]
        .map { _.response.status shouldBe StatusCodes.SeeOther }
      t3 <- doAction("get", "forest", Map(), keyValues = List("OF2"))
        .map { _ shouldBe Map("trees" -> "pine", "area" -> 23.5, "owner" -> "Pedro", "nr" -> "OF2") }
    } yield {
      t1
    }
  }

  it should "last step assignment returns call data" in {
    for {
      t1 <- doAction("get", "last_step_assignment_test",
        Map("param" -> "value", "nr" -> "#1"))
        .map { _ shouldBe MapResult(Map("param" -> "value", "nr" -> "#1", "value" -> "value")) }
    } yield {
      t1
    }
  }

  it should "fill data in loop with http requests" in {
    for {
      t1 <- doAction("insert", "tree",
        Map(
          "forest" -> "OF1",
          "plant_date" -> java.sql.Date.valueOf("2000-01-01"),
          "height" -> 5.3,
          "diameter" -> 1
        ))
      t2 <- doAction("get", "owner_with_forest_with_trees", Map("name" -> "Pedro"))
        .mapTo[MapResult]
        .map {
          _.result shouldBe ( YamlUtils.parseYamlData(
            """
               name: Pedro
               address: Morocco
               forests:
               - nr: OF1
                 owner: Pedro
                 area: 20.50
                 trees: oaks
                 tree_list:
                 - id: 20
                   forest: OF1
                   plant_date: 2000-01-01
                   height: 5.3
                   diameter: 1.0
               - nr: OF2
                 owner: Pedro
                 area: 23.50
                 trees: pine
                 tree_list: []
            """
          ))
        }
    } yield {
      t2
    }
  }

  it should "process not found properly" in {
    for {
      t1 <- doAction("get", "not_found_test", Map("name" -> "Zizo"))
        .map { _ shouldBe StatusResult(404, StringStatus("not found")) }
      t2 <- doAction("get", "not_found_test", Map("name" -> "Pedro"))
        .map {
          _ shouldBe Map("name" -> "Pedro", "address" -> "Morocco")
        }
      t3 <- doAction("get", "not_found_test_2", Map("name" -> "Zizo"))
        .map { _ shouldBe StatusResult(404, StringStatus("not found")) }
      t4 <- doAction("get", "not_found_test_2", Map("name" -> "Pedro"))
        .map {
          _ shouldBe Map("name" -> "Pedro", "address" -> "Morocco")
        }
    } yield {
      t1
    }
  }

  it should "extract http headers" in {
    for {
      t1 <- doAction("list", "extract_http_header_test", Map())
        .map { _ shouldBe MapResult(Map("h1_h2" -> "header1_value header2_value", "h3" -> null)) }
    } yield {
      t1
    }
  }
}
