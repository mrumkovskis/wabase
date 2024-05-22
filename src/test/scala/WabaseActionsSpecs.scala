package org.wabase

import akka.actor.ActorSystem
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, MessageEntity, StatusCodes}
import akka.http.scaladsl.server.{RequestContext, Route}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.scaladsl.StreamConverters
import akka.util.ByteString
import org.mojoz.querease.{TresqlMetadata, ValidationException, ValidationResult}
import org.scalatest.flatspec.{AsyncFlatSpec, AsyncFlatSpecLike}
import org.scalatest.matchers.should.Matchers
import org.tresql.{MissingBindVariableException, Query, ThreadLocalResources, convString}
import org.wabase.QuereaseActionsDtos.PersonWithHealthDataHealth

import java.io.File
import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import scala.collection.immutable.{ListMap, Seq}
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._
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

object WabaseActionsSpecs {
  class MailBox {
    /* use concurrent map since immutable map var declaration may lead to race condition errors. */
    val emails: scala.collection.concurrent.Map[String, Map[String, _]] =
      new ConcurrentHashMap[String, Map[String, _]]().asScala
  }

  class TestEmailSender(mailBox: MailBox) extends WabaseEmail {
    def sendMail(
      to: String,
      subject: String,
      body: String,
      attachments: Seq[EmailAttachment] = Nil,
      cc: String = null,
      bcc: String = null,
      from: String = null,
      replyTo: String = null,
      html: Boolean = false, /** Send body as plain text   */
      async: Boolean = true, /** Asynchronous sending flag */
    )(implicit
      ec: ExecutionContext,
      as: ActorSystem,
    ): Future[Unit] = {
      Future.traverse(attachments) { att =>
        att.content.runFold(ByteString.empty)(_ ++ _)
          .map(_.decodeString("UTF8"))(ec)
          .map(d => (att.filename, att.content_type, d))(ec)
      }.map { att =>
        val email = Map(
          "to" -> to,
          "cc" -> cc,
          "bcc" -> bcc,
          "from" -> from,
          "replyTo" -> replyTo,
          "subject" -> subject,
          "body" -> body,
          "attachments" -> att,
        )
        mailBox.emails += (to -> email)
      }
    }
  }
}

class WabaseActionsSpecs extends AsyncFlatSpec with Matchers with TestQuereaseInitializer with AsyncFlatSpecLike
  with ScalatestRouteTest {
  import WabaseActionsSpecs._

  val mailBox = new MailBox

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

      override protected lazy val doHttpRequest: HttpRequest => Future[HttpResponse] =
        Route.toFunction(service.route)(service.system)(_)

      override lazy val macrosClass: Class[_] = classOf[Macros]

      override def createEmailSender: WabaseEmail = new TestEmailSender(mailBox)
    }
    qio = new AppQuereaseIo[Dto](querease)
    super.beforeAll()
    val db = new DbAccess with Loggable {
      override val DefaultCp: PoolName = PoolName("wabase_db")
      override implicit val tresqlResources: ThreadLocalResources = WabaseActionsSpecs.this.tresqlThreadLocalResources
      override protected def tresqlMetadata: TresqlMetadata = WabaseActionsSpecs.this.querease.tresqlMetadata
    }
    app = new TestApp with NoValidation {
      override val DefaultCp: PoolName = PoolName("wabase_db")
      override protected val fileStreamerConnectionPool: PoolName = DefaultCp
      override def dbAccessDelegate = db
      override protected def initQuerease = querease
      override protected def shouldAddResultToContext(context: AppActionContext): Boolean =
        Set("result_audit_test") contains context.viewName

      override protected def afterWabaseAction(context: AppActionContext, result: Try[QuereaseResult]): Unit =
        if (context.viewName == "result_audit_test") {
          val res = context.serializedResult
          implicit val as = marshallers.system
          BorerNestedArraysTransformer
            .blockingTransform(res,
              JsonResultRenderer(_, false, new ResultRenderer.ViewFieldFilter(context.viewName, qe.nameToViewDef)))
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
  private implicit val user: TestUsr = TestUsr(100)
  private implicit val timeout: QueryTimeout = QueryTimeout(10)
  private implicit val defaultCp: PoolName = PoolName(dbNamePrefix)
  private implicit val as: ActorSystem = ActorSystem("wabase-action-specs")

  protected def doAction[T](action: String,
                            view: String,
                            values: Map[String, Any],
                            env: Map[String, Any] = Map.empty,
                            params: Map[String, Any] = Map.empty,
                            removeIdsFlag: Boolean = true,
                            keyValues: Seq[Any] = Nil,
                          ) = {
    implicit val state = ApplicationState(env)
    implicit val fileStreamer: AppFileStreamer[TestUsr] = app
    implicit val reqCtx: RequestContext = null
    app.doWabaseAction(action, view, keyValues, params, values)
      .map(_.result)
      .flatMap {
        case sr: QuereaseSerializedResult =>
          val filter =
            if (sr.resultFilter == null) new ResultRenderer.ViewFieldFilter(view, app.qe.nameToViewDef)
            else sr.resultFilter
          implicit val marshaller     = marshallers.toEntityQuereaseSerializedResultMarshaller(view, filter)
          Marshal(sr).to[MessageEntity]
            .flatMap { entity =>
              if (filter.name == view) {
                implicit val unmarshaller_1 = marshallers.toMapUnmarshallerForView(view)
                implicit val unmarshaller_2 = marshallers.toSeqOfMapsUnmarshallerForView(view)
                if  (sr.isCollection)
                  Unmarshal(entity).to[Seq[Map[String, Any]]]
                else Unmarshal(entity).to[Map[String, Any]]
              }else Future.successful {
                val in = entity.dataBytes.runWith(StreamConverters.asInputStream(1.second))
                new CborOrJsonAnyValueDecoder().decodeFromInputStream(in)
              }
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

  private implicit val state: ApplicationState = ApplicationState(Map())

  behavior of "metadata"

  it should "compile metadata" in {
    var msgs: List[String] = Nil
    app.qe.compileAllQueries(Set(), true, msgs ::= _)
    msgs.head should include ("compilation done")
  }

  behavior of "actions"

  // this is deprecated use wabase action calls
  it should "purchase without validation" in {
    val purchase = Map(
      "customer" -> "Ravus",
      "purchase_time" -> java.sql.Timestamp.valueOf("2021-12-04 00:06:53"),
      "item" -> "sword",
      "amount" -> 100
    )
    val id = app.save("purchase", purchase.toJson(app.qio.MapJsonFormat).asJsObject)
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

  it should "retrieve person purchase data from db use block" in {
    for {
      t1 <- doAction("get", "person_and_shop", Map("name" -> "Mr. Gunza"))
        .mapTo[MapResult]
        .map (_.result should be ( YamlUtils.parseYamlData(
          """
          name: Mr. Gunza
          birthdate: 1999-04-23
          purchases:
          - purchase_time: 2021-12-04 15:15:23.0
            item: joystick
            amount: 60.00
          vaccines:
          - manipulation_date: 2021-06-05
            vaccine: AstraZeneca
        """
        )))
    } yield t1
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
      t3 <- recoverToExceptionIf[Exception](doAction("delete", "invocation_test_1", Map())).map {
        _.getMessage should include ("Multiple methods 'ambiguousMethod' found")
      }
      t4 <- doAction("count", "invocation_test_1", Map()).map {
        _ shouldBe StringResult("0")
      }
      t4 <- doAction("get", "invocation_test_2", Map()).map {
        _ shouldBe Map("key" -> "key_val", "value" -> "value_val")
      }
      t5 <- doAction("list", "invocation_test_2", Map()).map {
        _ shouldBe List(Map("key" -> "key_val", "value" -> "value_val"))
      }
      t6 <- doAction("insert", "invocation_test_2", Map()).map {
        _ shouldBe Seq(Map("key" -> "key_val", "value" -> "value_val"))
      }
      t7 <- doAction("update", "invocation_test_2", Map()).map {
        _ shouldBe Seq(Map("key" -> "key_val", "value" -> "value_val"))
      }
      t8 <- doAction("list", "invocation_result_mapper_test", Map()).map {
        _ shouldBe List(Map("person_name" -> "N1 S1"), Map("person_name" -> "N2 S2"), Map("person_name" -> "N3 S3"))
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
      t7 <- doAction("get", "if_test_1", Map("code" -> "true")).map {
        _ shouldBe MapResult(ListMap("code" -> "true", "parent" -> null, "value" -> "Value"))
      }
      t8 <- doAction("get", "if_test_1", Map("code" -> "false")).map {
        _ shouldBe MapResult(ListMap("code" -> "false", "parent" -> null, "value" -> "Else value"))
      }
      t9 <- doAction("delete", "if_test_1", Map("code" -> "true")).map {
        _ shouldBe MapResult(ListMap("code" -> "true", "parent" -> null, "value" -> "Value delete"))
      }
      t10 <- doAction("delete", "if_test_1", Map("code" -> "false")).map {
        _ shouldBe MapResult(ListMap("code" -> "false", "parent" -> null, "value" -> "Else value delete"))
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
      t5 <- doAction("list", "foreach_test_3", Map()).map {
        _ shouldBe List(Map("nr" -> 1, "value" -> "value1"), Map("nr" -> 2, "value" -> "value2"), Map("nr" -> 3, "value" -> "value3"))
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
        }.map { _ shouldBe MapResult(Map("name" -> "Pedro", "address" -> "Morocco")) }
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
      t8 <- doAction("delete", "http_test_1", Map()). map {
        _ shouldBe StringResult("/count:invocation_test_1 = 0")
      }
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

  it should "return call data if last step is an assignment" in {
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
          _ shouldBe MapResult(Map("name" -> "Pedro", "address" -> "Morocco"))
        }
    } yield {
      t1
    }
  }

  it should "extract http headers" in {
    for {
      t1 <- doAction("list", "extract_http_header_test", Map())
        .map { _ shouldBe MapResult(Map(
          "h1_h2" -> "header1_value header2_value",
          "h_var" -> "header1_value header2_value",
          "h3" -> null))
        }
    } yield {
      t1
    }
  }

  it should "assign value to variable path" in {
    for {
      t1 <- doAction("get", "variable_path_test", Map())
        .map { _ shouldBe MapResult(
          Map("f1" -> "f1 val", "f2" -> Map("c1" -> "c1 val", "c2" -> Map("gc1" -> "gc1 val")))
        )}
      t2 <- doAction("get", "variable_path_test",
        Map("f1" -> "c1 par val", "f2" -> Map("c2" -> Map("gc1" -> "x"))))
        .map { _ shouldBe MapResult(
          Map("f1" -> "c1 par val", "f2" -> Map("c1" -> "c1 val", "c2" -> Map("gc1" -> "gc1 val")))
        )}
    } yield {
      t1
    }
  }

  it should "apply column filter to optional columns for get" in  {
    val person = Map(
      "birthdate" -> new java.sql.Date(Format.parseDate("1988-09-20").getTime),
      "sex" -> "M",
      "name" -> "CfGetName",
      "surname" -> "CfGetSurname"
    )
    for {
      id <-
        doAction("insert", "cols_filter_test_1", person)
          .map { case kr: KeyResult => kr.ir.id case _ => -1 }
      t0 <-
        doAction("get", "cols_filter_test_1", Map("id" -> id), Map.empty)
          .map( _ shouldBe person)
      t1 <-
        doAction("get", "cols_filter_test_1", Map("id" -> id), Map.empty, Map("cols" -> ""))
          .map( _ shouldBe (person - "surname" - "birthdate" - "sex"))
      t2 <-
        doAction("get", "cols_filter_test_1", Map("id" -> id), Map.empty, Map("cols" -> "name"))
          .map( _ shouldBe (person - "surname" - "birthdate" - "sex"))
      t3 <-
        doAction("get", "cols_filter_test_1", Map("id" -> id), Map.empty, Map("cols" -> "name, sex"))
          .map( _ shouldBe (person - "surname" - "birthdate"))
      t4 <-
        doAction("get", "cols_filter_test_1", Map("id" -> id), Map.empty, Map("cols" -> "name, surname"))
          .map( _ shouldBe (person - "birthdate" - "sex"))
      t5 <-
        doAction("get", "cols_filter_test_1", Map("id" -> id), Map.empty, Map("cols" -> "sex"))
          .map( _ shouldBe (person - "surname" - "birthdate"))
      cleanup <-
        doAction("delete", "cols_filter_test_1", Map("id" -> id))
    } yield t5
  }

  it should "apply column filter to optional columns for list" in  {
    val person = Map(
      "birthdate" -> new java.sql.Date(Format.parseDate("1988-09-20").getTime),
      "sex" -> "M",
      "name" -> "CfGetName",
      "surname" -> "CfGetSurname"
    )
    def listAndFind(id: Any, cols: String) =
      doAction("list", "cols_filter_test_1", Map.empty, Map.empty, Map("cols" -> cols), removeIdsFlag = false)
        .map(_.asInstanceOf[Seq[Map[String, Any]]].find(_("id") == id).map(_ - "id").get)
    for {
      id <-
        doAction("insert", "cols_filter_test_1", person)
          .map { case kr: KeyResult => kr.ir.id case _ => -1 }
      t0 <-
        listAndFind(id, null)
          .map( _ shouldBe person)
      t1 <-
        listAndFind(id, "")
          .map( _ shouldBe (person - "surname" - "birthdate" - "sex"))
      t2 <-
        listAndFind(id, "name")
          .map( _ shouldBe (person - "surname" - "birthdate" - "sex"))
      t3 <-
        listAndFind(id, "name, sex")
          .map( _ shouldBe (person - "surname" - "birthdate"))
      t4 <-
        listAndFind(id, "name, surname")
          .map( _ shouldBe (person - "birthdate" - "sex"))
      t5 <-
        listAndFind(id, "sex")
          .map( _ shouldBe (person - "surname" - "birthdate"))
      cleanup <-
        doAction("delete", "cols_filter_test_1", Map("id" -> id))
    } yield t5
  }

  it should "render template" in {
    for {
      t1 <-
        doAction("get", "template_test1", Map("name" -> "Dzidzis"))
          .map(_ shouldBe StringTemplateResult("Hello Dzidzis!"))
      t2 <-
        doAction("list", "template_test1", Map("name" -> "Anne"))
          .map(_ shouldBe StringTemplateResult("Hello Ms. Anne!"))
      t3 <-
        doAction("insert", "template_test1", Map("name" -> "Boris"))
          .map { r =>
            r shouldBe a [FileTemplateResult]
            val fa = r.asInstanceOf[FileTemplateResult]
            fa.filename shouldBe "file name"
            new String(fa.content) shouldBe "Hello Boris!"
          }
      t4 <-
        doAction("update", "template_test1", Map("name" -> "Joe"))
          .map { r =>
            r shouldBe a[FileTemplateResult]
            val fa = r.asInstanceOf[FileTemplateResult]
            fa.filename shouldBe "file name"
            new String(fa.content) shouldBe "Hello Joe in update!"
          }
      t5 <-
        doAction("delete", "template_test1", Map("name" -> "Migel"))
          .map(_ shouldBe StringTemplateResult("Hello Migel in delete!"))
    } yield t5
  }

  it should "send email" in {
    for {
      t1 <-
        doAction("insert", "email_test1", Map())
          .map { sentCount =>
            sentCount shouldBe LongResult(2)
            mailBox.emails shouldBe Map(
              "a@a.a" -> Map(
                "body" -> "Content for Hannah.",
                "subject" -> "Subject for Hannah!",
                "to" -> "a@a.a",
                "replyTo" -> null,
                "from" -> null,
                "bcc" -> null,
                "cc" -> null,
                "attachments" -> List(
                  (null, "text/plain; charset=UTF-8", "attachment from http for Hannah"),
                  ("file_attachment", "application/json", """[{"attachment":"attachment from file"}]"""),
                  ("attachment name", "text/plain; charset=UTF-8", "Template attachment for Hannah")
                )
              ),
              "b@b.b" -> Map(
                "body" -> "Content for Baiba.",
                "subject" -> "Subject for Baiba !",
                "to" -> "b@b.b",
                "replyTo" -> null,
                "from" -> null,
                "bcc" -> null,
                "cc" -> null,
                "attachments" -> List(
                  (null, "text/plain; charset=UTF-8", "attachment from http for Baiba"),
                  ("file_attachment", "application/json", """[{"attachment":"attachment from file"}]"""),
                  ("attachment name", "text/plain; charset=UTF-8", "Template attachment for Baiba")
                )
              )
            )
          }
    } yield t1
  }

  it should "not decode request" in {
    for {
      t1 <-
        doAction("update", "not_decode_request_insert_test", Map())
          .map(_ shouldBe StringResult("ok"))
      t2 <-
        doAction("insert", "not_decode_request_update_test", Map())
          .map(_ shouldBe StringResult("ok"))
    } yield t2
  }

  it should "decode form url encoded request" in {
    for {
      t1 <-
        doAction("get", "form_urlencoded_test", Map("name" -> "Nicola", "surname" -> "Ola"))
          .map(_ shouldBe StringResult("Nicola Ola"))
    } yield t1
  }

  it should "process config" in {
    for {
      t1 <-
        doAction("insert", "conf_test", Map())
          .map(_ shouldBe MapResult(Map("list" -> List(1, 2, 3), "uri" -> "http://wabase.org/")))
      t2 <-
        doAction("update", "conf_test", Map())
          .map(_ shouldBe StringResult("http://wabase.org/"))
    } yield t2
  }

  it should "build cursors" in {
    for {
      t1 <-
        doAction("list", "build_cursors_test", Map("books" ->
          List(
            Map("title" -> "Bear book", "year" -> 1971),
            Map("title" -> "Cat book", "year" -> 1971)
          )
        )).map(_ shouldBe List(
          Map("title" -> "Bear book", "year" -> 1971),
          Map("title" -> "Cat book", "year" -> 1971)
        ))
      t2 <- doAction("get", "build_cursors_test", Map())
        .map(_ shouldBe Map("title" -> "(OF1, Pedro),(OF2, Pedro)", "year" -> null))
    } yield t1
  }

  it should "call job" in {
    for {
      t1 <-
        doAction("get", "job_call_test1", Map())
          .map(_ shouldBe StatusResult(200, StringStatus("Hello from test_job1!")))
      t2 <-
        doAction("get", "job_call_test1", Map("name" -> "John"))
          .map(_ shouldBe StatusResult(200, StringStatus("Hello John from test_job1!")))
    } yield t1
  }

  it should "get resource" in {
    for {
      t1 <-
        doAction("get", "resource_test1", Map())
          .mapTo[ResourceResult]
          .flatMap { resource =>
            StreamConverters.fromInputStream(() =>
                classOf[Marshalling].getResource(resource.resource).openStream())
              .runReduce(_ ++ _)
          }
          .map(_.decodeString("UTF-8"))
          .map(_ should include("This is test resource!"))
    } yield t1
  }

  it should "render result with conversion" in {
    for {
      t1 <-
        doAction("insert", "result_render_test", Map())
          .map(_ shouldBe
            MapResult(Map("string_field" -> "text", "date_field" -> "2024-01-31"))
          )
      t2 <-
        doAction("get", "result_render_test", Map())
          .map(_ shouldBe
            List(
              Map("string_field" -> "text1", "date_field" -> null, "number_field" -> null),
              Map("string_field" -> "text2", "date_field" -> null, "number_field" -> null))
          )
      t3 <-
        doAction("update", "result_render_test", Map())
          .map(_ shouldBe MapResult(Map("string_field" -> "string")))
      t4 <-
        doAction("delete", "invocation_test_3", Map())
          .map {
            _ shouldBe AnyResult(Map("1" -> Map("key" -> "value")))
          }
      t5 <-
        doAction("delete", "result_render_test", Map())
          .map(_ shouldBe
            List(Map("string_field" -> "text", "date_field" -> "2024-01-31", "filtered_field" -> "x", "children" ->
              List(Map("child" -> "child"))))
          )
      t6 <-
        doAction("count", "result_render_test", Map())
          .map(_ shouldBe
            Map("string_field" -> "text", "date_field" -> "2024-01-31", "filtered_field" -> "x", "children" ->
              List(Map("child" -> "child")))
          )
      t7 <- recoverToExceptionIf[BusinessException](doAction("insert", "invocation_test_3", Map()))
        .map {
          _.getMessage shouldBe "Invocation error"
        }
    } yield t1
  }

  it should "do wabase routes and return results" in {
    implicit val user: TestUsr = TestUsr(100)
    /*
    * Cannot test since resources marshaller since 'resource.txt' file used in test not found from classloader which is used by
    * akka-http FileAndResourceDirectives.getFromResource method.
    *
    * classOf[Marshalling].getResource("/resource.txt") - WORKS
    * classOf[Marshalling].getClassLoader.getResource("/resource.txt") - DOES NOT WORK
    */
    val route = service.crudAction
    def jsonAssert(jsonStr: String, res: Any) =
      new CborOrJsonAnyValueDecoder().decode(ByteString(jsonStr)) shouldBe res
    Delete("/invocation_test_3") ~> route ~> check {
      val r = entityAs[String]
      jsonAssert(r, Map("1" -> Map("key" -> "value")))
    }
    Get("/create/invocation_test_3") ~> route ~> check {
      val r = entityAs[String]
      jsonAssert(r, List(Map("1" -> List(Map("key1" -> "value1"))), Map("2" -> List(Map("key2" -> "value2")))))
    }
    Get("/invocation_test_3/0") ~> route ~> check {
      val r = entityAs[String]
      jsonAssert(r, Seq(1, 2, 3))
    }
    Get("/invocation_test_3") ~> route ~> check {
      val r = entityAs[String]
      jsonAssert(r, Seq(Map("name" -> "Kizis"), Map("name" -> "Ala"), Map("name" -> "Ola")))
    }
    Get("/count/invocation_test_3") ~> route ~> check {
      val r = entityAs[String]
      jsonAssert(r, Seq(
        Map("id" -> 1, "name" -> "Kizis", "sex" -> "M", "birthdate" -> "1977-04-10"),
        Map("id" -> 2, "name" -> "Ala", "sex" -> "F", "birthdate" -> "1955-07-01"),
        Map("id" -> 3, "name" -> "Ola", "sex" -> "F", "birthdate" -> "1988-10-09"),
      ))
    }
  }
}
