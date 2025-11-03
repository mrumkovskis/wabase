package org.wabase

import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.marshalling.Marshal
import org.apache.pekko.http.scaladsl.model.HttpHeader.ParsingResult.Ok
import org.apache.pekko.http.scaladsl.model.headers.`Set-Cookie`
import org.apache.pekko.http.scaladsl.model.{AttributeKey, ContentType, ContentTypes, HttpEntity, HttpHeader, HttpRequest, HttpResponse, MessageEntity, Multipart, StatusCodes}
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.testkit.ScalatestRouteTest
import org.apache.pekko.http.scaladsl.unmarshalling.Unmarshal
import org.apache.pekko.stream.scaladsl.{Source, StreamConverters}
import org.apache.pekko.util.ByteString
import org.mojoz.querease.{TresqlMetadata, ValidationException, ValidationResult}
import org.mojoz.querease.ValueConverter.ClassOfJavaSqlDate
import org.scalatest.flatspec.{AsyncFlatSpec, AsyncFlatSpecLike}
import org.scalatest.matchers.should.Matchers
import org.tresql.{MissingBindVariableException, Query, ThreadLocalResources, convString}
import org.wabase.QuereaseActionsDtos.PersonWithHealthDataHealth
import org.wabase.client.WabaseHttpClient

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
    override def initFileStreamer: TestApp = WabaseActionsSpecs.this.app
  }

  var app: TestApp = _
  var marshallers: AppProvider[TestUsr] with QuereaseMarshalling with Execution = _
  var service: WabaseActionsService = _
  var wabaseScheduler: WabaseScheduler = _

  override def beforeAll(): Unit = {
    querease = new TestQuerease("/querease-action-specs-metadata.yaml") {
      override lazy val viewNameToClassMap = QuereaseActionsDtos.viewNameToClass ++ WabaseActionDtos.viewNameToClass

      override lazy val macrosClass: Class[_] = classOf[Macros]

      override def createEmailSender: WabaseEmail = new TestEmailSender(mailBox)
    }
    qio = new AppQuereaseIo[Dto](querease)
    super.beforeAll()
    val db = new DbAccess with QuereaseProvider with Loggable {
      override val DefaultCp: PoolName = PoolName("wabase_db")
      override implicit val tresqlResources: ThreadLocalResources = WabaseActionsSpecs.this.tresqlThreadLocalResources
      override protected def tresqlMetadata: TresqlMetadata = WabaseActionsSpecs.this.querease.tresqlMetadata
      override protected def initQuerease: AppQuerease = querease
      override protected def initQuereaseIo: AppQuereaseIo[Dto] = new AppQuereaseIo[Dto](querease)
    }
    app = new TestApp {
      override val DefaultCp: PoolName = PoolName("wabase_db")
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
              db.newTransaction(DefaultCp) { r =>
                Query("+simple_table {id = ?, value = ?}", id, bytes.decodeString("UTF-8"))(r)
              }
            }(scala.concurrent.ExecutionContext.global) // do not use AsyncFlatSpec context so that no blocking occurs
        }

      private val root_path =
        new File(System.getProperty("java.io.tmpdir"), "wabase-actions-specs/" + UUID.randomUUID().toString).getPath
      override lazy val fileStreamerConfig =
        ConfigFactory.parseString(s"files.path = $root_path")
          .withFallback(FileStreamerConfig.configs("main"))
      override implicit lazy val fileStreamers: WabaseFileStreamers = WabaseFileStreamers(
        Map("main" -> app.fileStreamer)
      )
      override implicit lazy val httpClients: WabaseHttpClients =
        WabaseHttpClients(Map("default-wabase-http-client" -> (_ => Route.toFunction(service.route)(service.system)(_))))
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
    wabaseScheduler = new WabaseScheduler(app, as)
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
    implicit val httpReq: HttpRequest = null
    app.doWabaseAction(action, view, keyValues, params, values)
      .map(_.result)
      .flatMap(processResult(_, view, removeIdsFlag))
      .transform(identity, { case e: QuereaseActionException => e.getCause case e => e })
  }

  protected def doJob(jobName: String, params: Map[String, Any]): Future[Any] = {
    wabaseScheduler.doJob(app.qe.viewDef(jobName), params)
  }

  protected def processResult(r: QuereaseResult, view: String, removeIdsFlag: Boolean): Future[Any] = r match {
    case sr@ResponseResult(_, ResultValue(value: QuereaseSerializedResult), _, _) =>
      processResult(value, view, removeIdsFlag).map(r => sr.copy(value = ResultValue(AnyResult(r))))
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
          } else Future.successful {
            val in = entity.dataBytes.runWith(StreamConverters.asInputStream(1.second))
            CborOrJsonAnyValueDecoder.decodeFromInputStream(in)
          }
        }
        .map(r => if (removeIdsFlag) removeIds(r) else r)
    case HttpResult(resp) => unmarshalResponse(resp)
    case CompatibleResult(m: MapResult, f, _) => Future.successful {
      if (f != null) MapResult(app.qe.toCompatibleMap(m.result, app.qe.viewDef(f.name)))
      else m
    }
    case r => Future.successful(r)
  }

  protected def unmarshalResponse(resp: HttpResponse): Future[Any] = {
    if (resp.status.isRedirection()) Future.successful {
      resp.headers.find(_.is("location")).map(_.value()).getOrElse("")
    } else resp.entity.toStrict(1.second)
      .map(_.data)
      .map { d =>
        Try(CborOrJsonAnyValueDecoder.decode(d))
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
    val id = app.save("purchase", purchase)
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
          r shouldBe ResponseResult(303, RedirectValue(TresqlUri.Uri(Seq("person_health"), List("Mr. Gunza", "2021-06-05"), ListMap("par1" -> "val1", "par2" -> "val2"))))
          doAction("save", "purchase", purchase)
        }
      }
    ).map(_.details should be(List(ValidationResult(Nil, List("Insufficient funds, available (0.00)")))))
  }

  it should "make purchase" in {
    val payment = Map(
      "beneficiary_name" -> "Mr. Gunza <no surname>",
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
      "beneficiary_name" -> "Mr. Mario <no surname>",
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
        r shouldBe ResponseResult(303, RedirectValue(TresqlUri.Uri(Seq("person_health"), List("Mr. Mario", "2021-08-15"), ListMap("par1" -> "val1", "par2" -> "val2"))))
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
              "birthdate" -> Format.convertToType("1988-09-20", ClassOfJavaSqlDate),
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
              "birthdate" -> Format.convertToType("1988-09-20", ClassOfJavaSqlDate),
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
                "birthdate" -> Format.convertToType("1988-09-20", ClassOfJavaSqlDate),
              )
            }
          }
        }
      t6 <-
        doAction("list", "env_test_1", Map("sex" -> "F")).map {
          _ shouldBe MapResult(Map("id" -> null, "name" -> "AddedName", "sex" -> "M", "birthdate" -> null))
        }
      t7 <-
        doAction("get", "env_test_5", Map("cond" -> true)).map {
          _ shouldBe MapResult(Map("x" -> 2, "y" -> 0, "a" -> 9, "b" -> 10, "cond" -> true))
        }
      t8 <-
        doAction("get", "env_test_5", Map("cond" -> false)).map {
          _ shouldBe MapResult(Map("cond" -> false, "x" -> 1, "y" -> -1, "z" -> 4))
        }
    } yield  {
      t1
    }
  }

  it should "return status" in {
    for {
      t1 <-
        doAction("get", "status_test_1", Map()).map {
          _ shouldBe ResponseResult(200, null)
        }
      t2 <-
        doAction("save", "status_test_1", Map("status" -> "ok")).map {
          _ shouldBe ResponseResult(200, ResultValue(StringResult("ok")))
        }
      t3 <-
        doAction("count", "status_test_1", Map("status" -> "ok")).map {
          _ shouldBe ResponseResult(200, ResultValue(AnyResult(NoResult)))
        }
      t4 <-
        doAction("list", "status_test_1", Map("status" -> "redirect")).map {
          _ shouldBe ResponseResult(303, RedirectValue(TresqlUri.Uri(Seq("/data"), List("path", "redirect"), ListMap())))
        }
      t5 <-
        doAction("get", "status_test_2", Map("id" -> 1)).map {
          _ shouldBe ResponseResult(303, RedirectValue(TresqlUri.Uri(Seq("data/path"), List("1"), ListMap())))
        }
      t6 <-
        doAction("save", "status_test_2", Map("id" -> 1)).map {
          _ shouldBe ResponseResult(303, RedirectValue(TresqlUri.Uri(Seq("data/path"), Nil, ListMap("id" -> "1"))))
        }
      t7 <-
        doAction("count", "status_test_2", Map()).map {
          _ shouldBe ResponseResult(303, RedirectValue(TresqlUri.Uri(Seq("data/path"), Nil, ListMap())))
        }
      t8 <-
        doAction("list", "status_test_2", Map()).map {
          _ shouldBe ResponseResult(303, RedirectValue(TresqlUri.Uri(Seq("data/path"), Nil, ListMap())))
        }
      t9 <-
        doAction("save", "status_test_3", Map()).map {
          _ shouldBe ResponseResult(303, RedirectValue(TresqlUri.Uri(Seq("303"), List(), ListMap())))
        }
      t10 <-
        doAction("get", "status_test_3", Map("id" -> 2)).map {
          _ shouldBe ResponseResult(303, RedirectValue(TresqlUri.Uri(Seq("data/path/2"), List(), ListMap())))
        }
      t11 <-
        doAction("list", "status_test_3", Map("id" -> 3)).map {
          _ shouldBe ResponseResult(303, RedirectValue(TresqlUri.Uri(Seq("data/path"), List("3"), ListMap("par1" -> "val-of-par1"))))
        }
      t12 <-
        doAction("count", "status_test_3", Map("id" -> 4)).map {
          _ shouldBe ResponseResult(303, RedirectValue(TresqlUri.Uri(Seq(null), List("4"), ListMap("par1" -> "5"))))
        }
      t13 <-
        doAction("save", "status_test_4", Map("id" -> null)).map {
          _ shouldBe ResponseResult(303, RedirectValue(TresqlUri.Uri(Seq("data/path"), List(null), ListMap())))
        }
      t14 <-
        doAction("get", "status_test_4", Map("id" -> null)).map {
          _ shouldBe ResponseResult(303, RedirectValue(TresqlUri.Uri(Seq("data/path"), List(), ListMap("id" -> null))))
        }
      t15 <-
        doAction("list", "status_test_4", Map("id" -> null)).map {
          _ shouldBe ResponseResult(303, RedirectValue(TresqlUri.Uri(Seq(null), List(), ListMap())))
        }
      t16 <-
        doAction("delete", "status_test_4", Map("id" -> null))
          .mapTo[ResponseResult]
          .map(_.value)
          .mapTo[ResultValue]
          .map(_.value)
          .mapTo[AnyResult]
          .map(_.result)
          .map {
            _ shouldBe List(Map("a" -> "a value", "b" -> "b value"))
          }
    } yield {
      t15
    }
  }

  it should "do invocations" in {
    for {
      t1 <- doAction("get", "invocation_test_1", Map()).map {
        _ shouldBe ResponseResult(200, ResultValue(StringResult("val1 val2")))
      }
      t2 <- doAction("save", "invocation_test_1", Map()).map {
        _ shouldBe MapResult(Map("nr" -> 2.5))
      }
      t3 <- doAction("count", "invocation_test_1", Map()).map {
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
      t8 <- doAction("delete", "invocation_test_2", Map("java_key" -> "java_value")).map {
        _ shouldBe MapResult(Map("java_key" -> "java_value"))
      }
      t9 <- doAction("count", "invocation_test_2", Map())
        .map(_ shouldBe StringResult("value value") )
      t10 <- doAction("list", "invocation_result_mapper_test", Map()).map {
        _ shouldBe List(Map("person_name" -> "N1 S1"), Map("person_name" -> "N2 S2"), Map("person_name" -> "N3 S3"))
      }
      t11 <- doAction("delete", "invocation_test_1", Map("n1" -> "1.2", "n2" -> "1.3")).map {
        _ shouldBe NumberResult(2.5)
      }
      t12 <- doAction("list", "invocation_test_4", Map()).map {
        _ shouldBe List(Map("key" -> "1", "value" -> "2"), Map("key" -> "a", "value" -> "b"))
      }
      t13 <- doAction("insert", "invocation_test_4", Map()).map {
        _ shouldBe List(Map("key" -> "3", "value" -> "4"), Map("key" -> "x", "value" -> "y"))
      }
    } yield {
      t2
    }
  }

  it should "do invocations with multiple arguments" in {
    for {
      t1 <- doAction("get", "multi_argument_invocation", Map()).map {
        _ shouldBe StringResult("a b c")
      }
      t2 <- doAction("insert", "multi_argument_invocation", Map("value" -> 1)).map {
        _ shouldBe StringResult("0 1 2")
      }
      t3 <- doAction("update", "multi_argument_invocation", Map("value" -> 2)).map {
        _ shouldBe StringResult("1 2 3")
      }
      t4 <- doAction("delete", "multi_argument_invocation", Map("value" -> 3)).map {
        _ shouldBe StringResult("2 3 1 2 3")
      }
      t5 <- doAction("insert", "invocation_type_conversion_test", Map()).map {
        _ shouldBe AnyResult(true)
      }
      t6 <- doAction("update", "invocation_type_conversion_test", Map()).map {
        _ shouldBe AnyResult(true)
      }
    } yield {
      t1
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
      t3 <- doAction("insert", "insert_to_no_result_test", Map("value" -> "insert_to_no_result_test")).map {
        _ shouldBe NoResult
      }
      t4 <- doAction("get", "insert_to_no_result_test", Map("value" -> "insert_to_no_result_test")).map {
        _ shouldBe Map("value" -> "insert_to_no_result_test")
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
        _ shouldBe ResponseResult(200, ResultValue(StringResult("yes")))
      }
      t4 <- doAction("get", "if_test_2", Map("value" -> false)).map {
        _ shouldBe ResponseResult(200, ResultValue(StringResult(null)))
      }
      t5 <- doAction("list", "if_test_2", Map("value" -> true)).map {
        _ shouldBe ResponseResult(200, ResultValue(StringResult("yes")))
      }
      t6 <- doAction("list", "if_test_2", Map("value" -> false)).map {
        _ shouldBe ResponseResult(200, ResultValue(StringResult(null)))
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
        app.dbAccess.withConn(app.DefaultCp) { implicit r =>
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
        .map { _ shouldBe "val1 val2" }
      t2 <- doAction("list", "http_test_2", Map())
        .map { _ shouldBe "val1 val2" }
      t3 <- doAction("insert", "http_test_2", Map())
        .map {
          _ shouldBe ResponseResult(200, ResultValue(StringResult("person_health?/Mr.%20Mario/2022-04-11?par1=val1&par2=val2")))
        }
      t4 <- doAction("update", "http_test_2", Map("name" -> "Mr. Gunza",
        "manipulation_date" -> "2022-09-10", "vaccine" -> "Pfizer"))
        .map { _ shouldBe "person_health?/Mr.%20Gunza/2022-09-10?par1=val1&par2=val2" }
      t5 <- doAction("insert", "forest", Map("nr" -> "OF1", "owner" -> "Pedro",
        "area" -> 1000, "trees" -> "oaks"))
        .map { case KeyResult(_, _, key) => key shouldBe List("OF1") }
      t6 <- doAction("update", "http_forest", Map("area" -> 20.5), keyValues = List("OF1"))
        .map { _ shouldBe Map("nr" -> "OF1") }
      t7 <- doAction("get", "forest", Map(), keyValues = List("OF1"))
        .map { _ shouldBe Map("nr" -> "OF1", "owner" -> "Pedro", "area" -> 20.5, "trees" -> "oaks") }
      t8 <- doAction("delete", "http_test_1", Map()). map {
        _ shouldBe StringResult("/count:invocation_test_1 = 0")
      }
      t9 <- doAction("get", "http_client_test", Map("uri" -> "/invocation_test_1")).map {
        _ shouldBe "val1 val2"
      }
      t10 <- doAction("insert", "http_test_3",
        Map("f1" -> 10, "f2" -> 20, "m1" -> Map("f1" -> "X", "f2" -> "Y"))).map {
        _ shouldBe Map("f1" -> 10, "f2" -> 20, "m1" -> Map("f1" -> "X", "f2" -> "Y"))
      }
      t11 <- doAction("update", "http_test_3",
        Map("f1" -> "F1", "f2" -> "F2", "m1" -> Map("f1" -> "A", "f2" -> "B"))).map {
        _ shouldBe Map("f1" -> "F1", "f2" -> "F2", "m1" -> Map("f1" -> "A", "f2" -> "B"))
      }
      t12 <- doAction("delete", "http_test_3",
        Map("f1" -> "FD1", "f2" -> "FD2", "m1" -> Map("f1" -> "AD", "f2" -> "BD"))).map {
        _ shouldBe Map("f1" -> "FD1", "f2" -> "FD2", "m1" -> Map("f1" -> "AD", "f2" -> "BD"))
      }
    } yield {
      t1
    }
  }

  it should "do json codec operation" in {
    def enc(v: Any) = ResultEncoder.encodeAnyToJsonString(v)
    for {
      t1 <- doAction("get", "json_codec_1", Map("value" ->
        enc(Map("trees" -> "pine", "area" -> 23.5, "notes" -> null))))
        .map { _ shouldBe MapResult(Map("trees" -> "pine", "area" -> 23.5, "notes" -> null)) }
      t2 <- doAction("insert", "json_codec_1", Map("value" ->
        enc(Map("trees" -> "pine", "area" -> 23.5, "owner" -> "Pedro", "nr" -> "OF2"))))
        .map { _ shouldBe Map("nr" -> "OF2") }
      t3 <- doAction("get", "forest", Map(), keyValues = List("OF2"))
        .map { _ shouldBe Map("trees" -> "pine", "area" -> 23.5, "owner" -> "Pedro", "nr" -> "OF2") }
      t4 <- doAction("get", "json_codec_2", Map())
        .map { _ shouldBe MapResult(Map("nr" -> "Nr1", "owner" -> "Owner5", "area" -> 12.4, "trees" -> "Fig")) }
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
        .map { r =>
          removeIds(r.result) shouldBe ( YamlUtils.parseYamlData(
            """
               name: Pedro
               address: Morocco
               forests:
               - nr: OF1
                 owner: Pedro
                 area: 20.50
                 trees: oaks
                 tree_list:
                 - forest: OF1
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
        .map { _ shouldBe NoResult }
      t2 <- doAction("get", "not_found_test", Map("name" -> "Pedro"))
        .map {
          _ shouldBe Map("name" -> "Pedro", "address" -> "Morocco")
        }
      t3 <- doAction("get", "not_found_test_2", Map("name" -> "Zizo"))
        .map { _ shouldBe ResponseResult(404, ResultValue(StringResult("not found"))) }
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

  it should "extract http cookies" in {
    for {
      t1 <- doAction("list", "extract_http_cookie_test", Map())
        .map { _ shouldBe MapResult(Map("lang_user" -> "lv dzidzis", "c_var" -> "lv dzidzis", "no_cookie" -> null)) }
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

  it should "define variable in the block" in {
    for {
      t1 <- doAction("get", "variable_definition_block_test", Map())
        .map (_ shouldBe MapResult(Map("a" -> "a", "b" -> "b", "c" -> Map("d" -> "d", "e" -> "e"))))
    } yield t1
  }

  it should "apply column filter to optional columns for get" in  {
    val person = Map(
      "birthdate" -> Format.convertToType("1988-09-20", ClassOfJavaSqlDate),
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
        doAction("get", "cols_filter_test_1", Map("id" -> id), Map.empty, Map("fields" -> ""))
          .map( _ shouldBe (person - "surname" - "birthdate" - "sex"))
      t2 <-
        doAction("get", "cols_filter_test_1", Map("id" -> id), Map.empty, Map("fields" -> "name"))
          .map( _ shouldBe (person - "surname" - "birthdate" - "sex"))
      t3 <-
        doAction("get", "cols_filter_test_1", Map("id" -> id), Map.empty, Map("fields" -> "name, sex"))
          .map( _ shouldBe (person - "surname" - "birthdate"))
      t4 <-
        doAction("get", "cols_filter_test_1", Map("id" -> id), Map.empty, Map("fields" -> "name, surname"))
          .map( _ shouldBe (person - "birthdate" - "sex"))
      t5 <-
        doAction("get", "cols_filter_test_1", Map("id" -> id), Map.empty, Map("fields" -> "sex"))
          .map( _ shouldBe (person - "surname" - "birthdate"))
      cleanup <-
        doAction("delete", "cols_filter_test_1", Map("id" -> id))
    } yield t5
  }

  it should "apply column filter to optional columns for list" in  {
    val person = Map(
      "birthdate" -> Format.convertToType("1988-09-20", ClassOfJavaSqlDate),
      "sex" -> "M",
      "name" -> "CfGetName",
      "surname" -> "CfGetSurname"
    )
    def listAndFind(id: Any, cols: String) =
      doAction("list", "cols_filter_test_1", Map.empty, Map.empty, Map("fields" -> cols), removeIdsFlag = false)
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
            sentCount shouldBe LongResult(0)
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
              ),
              "c@c.c" -> Map(
                "body" -> "Content for Minna.",
                "subject" -> "Subject for Minna!",
                "replyTo" -> "r@r.r",
                "to" -> "c@c.c",
                "from" -> "f@f.f",
                "bcc" -> null,
                "cc" -> "c1@.c1.c1",
                "attachments" -> Nil),
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

  it should "use evaluator for http request builder" in {
    for {
      t1 <-
        doAction("get", "form_urlencoded_with_evaluator_test", Map("name" -> "Nicola", "surname" -> "Ola"))
          .map(_ shouldBe StringResult("Nicola Ola"))
    } yield t1
  }

  it should "handle empty status message correctly" in {
    for {
      t1 <-
        doAction("get", "form_urlencoded_empty_status_test", Map("name" -> "Nicola", "surname" -> "Ola"))
          .map(_ shouldBe StringResult(""))
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
          .map(_ shouldBe ResponseResult(200, ResultValue(StringResult("Hello from test_job1!"))))
      t2 <-
        doAction("get", "job_call_test1", Map("name" -> "John"))
          .map(_ shouldBe ResponseResult(200, ResultValue(StringResult("Hello John from test_job1!"))))
      t3 <- doJob("test_job_insert", Map()).map(_ shouldBe NoResult)
      t4 <- doAction("list", "job_call_test1", Map("name" -> "ABC"))
        .map(_ shouldBe List(Map("value" -> "ABC")))
      t5 <- doAction("insert", "job_call_test1",
        Map("id" -> 1, "ch" ->
          List(
            Map("id" -> 2, "ch" -> List(Map("id" -> 4, "ch" -> Nil))),
            Map("id" -> 3, "ch" -> Nil)
          )
        )
      )
        .map(_ shouldBe MapResult(
          Map("id" -> 2, "ch" ->
            List(
              Map("id" -> 3, "ch" -> List(Map("id" -> 5, "ch" -> Nil))),
              Map("id" -> 4, "ch" -> Nil)
            )
          )
        ))
      t6 <- doAction("update", "job_call_test1",
        Map("data" -> Map("id" -> 1, "ch" -> List(Map("id" -> 2, "ch" -> Nil))))
      )
        .map(_ shouldBe MapResult(Map("id" -> 2, "ch" -> List(Map("id" -> 3, "ch" -> Nil)))))
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
      t8 <- recoverToExceptionIf[IllegalArgumentException](doAction("update", "invocation_test_3", Map()))
        .map {
          _.getMessage should include (": java.lang.String") // cannot test 'str: java.lang.String' because for scala 3 parameter name is arg0
        }
    } yield t1
  }

  it should "return this" in {
    for {
      t1 <- doAction("get", "this_test1", Map())
        .map(_ shouldBe MapResult(Map("name" -> "John", "surname" -> "Telos")))
    } yield t1
  }

  it should "use evaluator db" in {
    for {
      t1 <- doAction("get", "evaluator_db_test", Map())
        //.mapTo[CompatibleResult]
        .map(_ shouldBe StringResult("Hello from wabase!"))
      t2 <- doAction("get", "evaluator_db_test1", Map())
        .map(_ shouldBe StringResult("set-basic-auth-credentials/123"))
    } yield t1
  }

  def decodeJs(js: String) = CborOrJsonAnyValueDecoder.decode(ByteString(js))
  def jsonAssert(jsonStr: String, res: Any) =
    decodeJs(jsonStr) shouldBe res
  def createEntity(content: String, ct: ContentType) = HttpEntity(ct, ByteString(content))
  def concatForms(forms: Multipart.FormData*) = Multipart.FormData(
    forms.foldLeft(Source.empty[Multipart.FormData.BodyPart])(_ ++ _.parts)
  )

  it should "do wabase routes and return results" in {
    implicit val user: TestUsr = TestUsr(100)
    /*
    * Cannot test since resources marshaller since 'resource.txt' file used in test not found from classloader which is used by
    * pekko-http FileAndResourceDirectives.getFromResource method.
    *
    * classOf[Marshalling].getResource("/resource.txt") - WORKS
    * classOf[Marshalling].getClassLoader.getResource("/resource.txt") - DOES NOT WORK
    */
    val route = service.crudAction
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
    Get("/fake_key_test/123/456") ~> route ~> check {
      val r = entityAs[String]
      jsonAssert(r, Map("id" -> 123, "sha_256" -> "456"))
    }
    Get("/count/invocation_test_3") ~> route ~> check {
      val r = entityAs[String]
      jsonAssert(r, Seq(
        Map("id" -> 1, "name" -> "Kizis", "sex" -> "M", "birthdate" -> "1977-04-10"),
        Map("id" -> 2, "name" -> "Ala", "sex" -> "F", "birthdate" -> "1955-07-01"),
        Map("id" -> 3, "name" -> "Ola", "sex" -> "F", "birthdate" -> "1988-10-09"),
      ))
    }
    Post("/extract_parts_test", concatForms(
      WabaseHttpClient.fileUploadForm(createEntity("Field value1", ContentTypes.`text/plain(UTF-8)`), null, "field1"),
      WabaseHttpClient.fileUploadForm(createEntity("Field value2", ContentTypes.`text/plain(UTF-8)`), null, "field2"),
      WabaseHttpClient.fileUploadForm(createEntity("Hi people!", ContentTypes.`text/plain(UTF-8)`), "test1.txt", "file1"),
      WabaseHttpClient.fileUploadForm(createEntity("How are you!", ContentTypes.`text/plain(UTF-8)`), "test2.txt", "file2"),
    )) ~> route ~> check {
      val r = entityAs[String]
      jsonAssert(r, Seq(
        Map("name" -> "field1", "value" -> "Field value1"),
        Map("name" -> "field2", "value" -> "Field value2"),
        Map("name" -> "test1.txt", "value" -> "228c55536f6bcca78166c30c29199c4b6a52c8ed560cdc1db62ec1ac8af5df30"),
        Map("name" -> "test2.txt", "value" -> "8be8a3875c871e2f3990640f18d914836152dfacfd34880a13358e04c6471ea9")
      ))
    }
    Put("/extract_parts_test/test-upd.txt", createEntity("Hi people again!", ContentTypes.`text/plain(UTF-8)`)) ~>
      route ~> check {
      val r = entityAs[String]
      jsonAssert(r, Map("file" -> "test-upd.txt", "sha_256" -> "4744b699460641b88c590e8a9c089970345ffcce056501652b06ec166c01b666"))
    }
    Post("/extract_parts_test2", WabaseHttpClient
      .fileUploadForm(createEntity("Hi people!", ContentTypes.`text/plain(UTF-8)`), "test.txt")) ~> route ~> check {
      val r = entityAs[String]
      jsonAssert(r, Seq(Map("file" -> "test.txt", "sha_256" -> "228c55536f6bcca78166c30c29199c4b6a52c8ed560cdc1db62ec1ac8af5df30")))
    }
    Post("/extract_parts_test2", WabaseHttpClient
      .fileUploadForm(createEntity("Hi people!" * 150, ContentTypes.`text/plain(UTF-8)`), "test.txt")) ~> route ~> check {
      status == StatusCodes.ContentTooLarge
    }
    var fileId: Any = null
    var fileSha: Any = null
    Post("/upload_test/file.txt", createEntity("upload download", ContentTypes.`text/plain(UTF-8)`)) ~>
      route ~> check {
        val r = decodeJs(entityAs[String]).asInstanceOf[Map[String, Any]]
        fileId = r("id")
        fileSha = r("sha_256")
        val filename = r("filename")
        filename shouldBe "file.txt"
      }
    Get(s"/download_test/$fileId/$fileSha") ~> route ~> check {
      val r = entityAs[String]
      r shouldBe "upload download"
    }
    Get(s"/http_with_limit_test/$fileId/$fileSha") ~> route ~> check {
      an [Exception] should be thrownBy entityAs[String]
    }
    Post("/custom_decoder_test", createEntity("id,value\n1,hello", ContentTypes.`text/plain(UTF-8)`)) ~>
      route ~> check {
      val r = entityAs[String]
      jsonAssert(r, List(Map("id" -> "1", "value" -> "hello")))
    }
    Post("/default_decoder_test",
      createEntity(ResultEncoder.encodeToJsonString(Map("id" -> "1", "value" -> "hello")), ContentTypes.`application/json`)) ~>
      route ~> check {
      val r = entityAs[String]
      jsonAssert(r, Map("id" -> "1", "value" -> "hello"))
    }
    Post("/default_decoder_test",
      createEntity("id=1&value=hello", ContentTypes.`application/x-www-form-urlencoded`)) ~>
      route ~> check {
      val r = entityAs[String]
      jsonAssert(r, Map("id" -> "1", "value" -> "hello"))
    }
  }

  it should "extract entity" in {
    implicit val user: TestUsr = TestUsr(100)
    val route = service.crudAction
    Put("/extract_http_entity_test1",
      HttpEntity(ContentTypes.`text/plain(UTF-8)`, ByteString("Good morning!"))) ~> route ~> check {
      val r = entityAs[String]
      r shouldBe "Good morning!"
    }
    Post("/extract_http_entity_test2",
      HttpEntity(ContentTypes.`text/plain(UTF-8)`, ByteString("Good afternoon!"))) ~> route ~> check {
      val r = entityAs[String]
      r shouldBe "Good afternoon!"
    }
    Post("/invocation_keep_result_test",
      HttpEntity(ContentTypes.`text/plain(UTF-8)`, ByteString("Good evening!"))) ~> route ~> check {
      val r = entityAs[String]
      r shouldBe "Good evening!"
    }
    Post("/extract_http_entity_test3",
      HttpEntity(ContentTypes.`application/json`, ByteString("""[{"name": "A"}, {"name": "B"}]"""))) ~> route ~> check {
      jsonAssert(entityAs[String], List(Map("name" -> "A A"), Map("name" -> "B B")))
    }
    Put("/extract_http_entity_test3",
      HttpEntity(ContentTypes.`application/json`, ByteString("""[{"name": "A"}, {"name": "B"}]"""))) ~> route ~> check {
      jsonAssert(entityAs[String], List(Map("name" -> "A-A"), Map("name" -> "B-B")))
    }
    Post("/extract_http_entity_test4", HttpEntity.Empty) ~> route ~> check {
      jsonAssert(entityAs[String], List(
        Map("name" -> "N1 N1", "surname" -> "S1 S1"),
        Map("name" -> "N2 N2", "surname" -> "S2 S2"))
      )
    }
    Put("/extract_http_entity_test4", HttpEntity.Empty) ~> route ~> check {
      jsonAssert(entityAs[String], List(
        Map("name" -> "N1-N1", "surname" -> "S1-S1"),
        Map("name" -> "N2-N2", "surname" -> "S2-S2"))
      )
    }
    Post("/extract_csv_http_entity_test", HttpEntity.Empty) ~> route ~> check {
      jsonAssert(entityAs[String], List(
        Map("name" -> "N1 N1", "surname" -> "S1 S1"),
        Map("name" -> "N2 N2", "surname" -> "S2 S2"))
      )
    }
    Put("/extract_csv_http_entity_test",
      HttpEntity(ContentTypes.`text/csv(UTF-8)`, ByteString("name,surname\nn1,s1\nn2,s2\n"))) ~> route ~> check {
      jsonAssert(entityAs[String],  List(
        Map("name" -> "n1-n1", "surname" -> "s1-s1"),
        Map("name" -> "n2-n2", "surname" -> "s2-s2"),
      ))
    }
    Post("/extract_xml_http_entity_test",
      HttpEntity(ContentTypes.`text/xml(UTF-8)`,
        ByteString(
          """
            |<data>
            |  <record>
            |    <name>Name1</name><surname>Surname1</surname>
            |  </record>
            |  <record>
            |    <name>Name2</name><surname>Surname2</surname>
            |  </record>
            |</data>
            |""".stripMargin))) ~> route ~> check {
      jsonAssert(entityAs[String],  List(
        Map("name" -> "Name1-Name1", "surname" -> "Surname1-Surname1"),
        Map("name" -> "Name2-Name2", "surname" -> "Surname2-Surname2"),
      ))
    }
    Put("/extract_xml_http_entity_test",
      HttpEntity(ContentTypes.`text/xml(UTF-8)`,
        ByteString(
          """
            |<data>
            |  <record>
            |    <name>Name1</name><surname>Surname1</surname>
            |  </record>
            |  <record>
            |    <name>Name2</name><surname>Surname2</surname>
            |  </record>
            |</data>
            |""".stripMargin))) ~> route ~> check {
      jsonAssert(entityAs[String],  List(
        Map("name" -> "Name1", "surname" -> "Surname1"),
        Map("name" -> "Name2", "surname" -> "Surname2"),
      ))
    }
  }

  it should "set response headers" in {
    implicit val user: TestUsr = TestUsr(4)
    val route = service.crudAction
    Get("/set_headers_test1") ~> route ~> check {
      headers.collect { case `Set-Cookie`(c) => (c.name, (c.value, c.path, c.expires.map(_.toString()))) }
        .toMap shouldBe Map(
          "deleme" -> ("", Some("test_path"), Some("1800-01-01T00:00:00")),
          "test" ->   ("test_val", None, Some("2025-02-02T23:10:05"))
        )
      header("header1") shouldBe Some(HttpHeader.parse("header1", "value1").asInstanceOf[Ok].header)
      header("header2") shouldBe Some(HttpHeader.parse("header2", "value2").asInstanceOf[Ok].header)
      response.attribute(AttributeKey[WabaseUser](WabaseService.WabaseUserAttributeName)) shouldBe Some(WabaseUser(Map("attr1" -> "val1", "attr2" -> "val2")))
    }
    Get("/set_headers_test2") ~> route ~> check {
      headers.collect {
        case `Set-Cookie`(c) => (c.name, (c.value, c.secure, c.httpOnly, c.maxAge, c.expires.map(_.toString()), c.domain))
      }.toMap shouldBe Map(
        "x" -> ("", false, false, None, Some("1800-01-01T00:00:00"), Some("abc.com")),
        "test" -> ("test_val", true, true, Some(1000), None, None),
        "test1" -> ("test_val1", false, false, None, Some("2025-04-03T13:30:25"), None)
      )
      header("h1") shouldBe Some(HttpHeader.parse("h1", "v1").asInstanceOf[Ok].header)
      header("h2") shouldBe Some(HttpHeader.parse("h2", "v2").asInstanceOf[Ok].header)
      header("location") shouldBe Some(HttpHeader.parse("location", "/redirect_path/view").asInstanceOf[Ok].header)
    }
    Get("/set_headers_test3") ~> route ~> check {
      header("response_header_name") shouldBe Some(HttpHeader.parse("response_header_name", "response_header_value").asInstanceOf[Ok].header)
      jsonAssert(entityAs[String], List(Map("RN" -> "r1", "C" -> "v1"), Map("RN" -> "r2", "C" -> "v2")))
    }
    Get("/set_headers_test4") ~> route ~> check {
      headers.collect {
        case `Set-Cookie`(c) => (c.name, (c.value, c.secure, c.httpOnly, c.expires.map(_.toString())))
      }.toMap shouldBe Map("my_cookie" -> ("my_cookie_value", false, false, Some("2025-03-05T09:37:40")))
      jsonAssert(entityAs[String], "http://wabase.org/")
    }
  }

  it should "bind querease results to tresql" in {
    implicit val user: TestUsr = TestUsr(100)
    val route = service.crudAction
    Post("/source_tresql_binding_test1",
      HttpEntity(ContentTypes.`text/plain(UTF-8)`, ByteString("This is stream!"))) ~> route ~> check {
      val r = entityAs[String]
      r shouldBe "This is stream!"
    }
    Put("/source_tresql_binding_test1", concatForms(
      WabaseHttpClient.fileUploadForm(createEntity("Stream field 1", ContentTypes.`text/plain(UTF-8)`), "file1", "f1"),
      WabaseHttpClient.fileUploadForm(createEntity("Stream field 2", ContentTypes.`text/plain(UTF-8)`), "file2", "f2"),
      WabaseHttpClient.fileUploadForm(createEntity("Stream field 3", ContentTypes.`text/plain(UTF-8)`), "file3", "f3"),
    )) ~> route ~> check {
      val r = entityAs[String]
      jsonAssert(r, Seq("Stream field 1", "Stream field 2", "Stream field 3"))
    }
  }

  it should "foreach as an expression" in {
    implicit val user: TestUsr = TestUsr(100)
    val route = service.crudAction
    Get("/count:foreach_test_3") ~> route ~> check {
      jsonAssert(entityAs[String], Seq(2, 3, 4))
    }
    Post("/foreach_test_3") ~> route ~> check {
      jsonAssert(entityAs[String], List(
        "foreach_test_1 top_upd", "foreach_test_1.ch_1 child1_upd",
        "foreach_test_1.ch_2 child2_upd", "foreach_test_2 old",
        "foreach_test_2.1 new", "foreach_test_2.1.ch_2 new child", "if_test_1 no_value"
      ))
    }
  }

  it should "return array" in {
    for {
      t1 <- doAction("get", "array_test1", Map())
        .map(_ shouldBe List(1, 2, 3))
      t2 <- doAction("list", "array_test1", Map())
        .map(_ shouldBe List("EnvTestName", "EnvTestName", "Mika", "Mr. Gunza", "Mr. Mario"))
      t3 <- doAction("insert", "array_test1", Map())
        .map(_ shouldBe List(Map("name" -> "jasmine", "roles" -> List("admin", "guest"))))
    } yield t1
  }

  behavior of "Save operation with dynamically generated deep nesting"

  it should "attempt to process a very deep structure and observe behavior" in {
    val nestingDepth = 3000
    def createDeepInputMap(depth: Int): Map[String, Any] = {
      Map(
        "code" -> "root",
        "value" -> "top_level",
        "children" -> createNestedChildrenData(1, depth)
      )
    }
    def createNestedChildrenData(currentDepth: Int, maxDepth: Int): List[Map[String, Any]] = {
      if (currentDepth > maxDepth) {
        List.empty[Map[String, Any]]
      } else {
        val childCode = s"code$currentDepth"
        List(
          Map(
            "code" -> childCode,
            "value" -> s"ch_val_$currentDepth",
            "children" -> createNestedChildrenData(currentDepth + 1, maxDepth)
          )
        )
      }
    }
    val deepInput = createDeepInputMap(nestingDepth)
    recoverToExceptionIf[IllegalStateException] {
      doAction("save", "foreach_test_1", deepInput)
    }.map(_.getMessage should startWith("Structure depth exceeds"))
  }
}
