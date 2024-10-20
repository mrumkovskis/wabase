package org.wabase

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.server.RequestContext
import org.mojoz.querease.{ValidationException, ValidationResult}
import org.scalatest.flatspec.{AsyncFlatSpec, AsyncFlatSpecLike}
import org.scalatest.matchers.should.Matchers
import org.tresql.{Query, Resources, convLong}
import org.wabase.QuereaseActionsDtos.Person

import scala.concurrent.{ExecutionContext, Future}

object QuereaseActionsDtos {
  class Person extends DtoWithId {
    var id: java.lang.Long = null
    var name: String = null
    var surname: String = null
    var sex: String = null
    var birthdate: java.sql.Date = null
    var main_account: String = null
    var accounts: List[PersonAccounts] = Nil
  }
  class PersonAccounts extends DtoWithId {
    var id: java.lang.Long = null
    var number: String = null
    var balance: BigDecimal = null
    var last_modified: java.sql.Timestamp = null
  }
  class PersonAccountsDetails extends DtoWithId {
    var id: java.lang.Long = null
    var name: String = null
    var surname: String = null
    var main_account: PersonAccounts = null
    var accounts: List[PersonAccounts] = Nil
    var balances: List[String] = null
  }
  class Payment extends DtoWithId {
    var id: java.lang.Long = null
    var originator: String = null
    var beneficiary: String = null
    var amount: BigDecimal = null
    var date_time: java.sql.Timestamp = null
  }
  class PersonList extends DtoWithId {
    var id: java.lang.Long = null
    var name: String = null
    var surname: String = null
    var sex: String = null
    var birthdate: java.sql.Date = null
  }
  class PersonWithMainAccount extends DtoWithId {
    var id: java.lang.Long = null
  }
  class PersonHealth extends DtoWithId {
    var id: java.lang.Long = null
    var name: String = null
    var manipulation_date: java.sql.Date = null
    var vaccine: String = null
    var had_virus: jBoolean = null
  }
  class PersonWithHealthData extends Dto {
    var name: String = null
    var sex: String = null
    var birthdate: java.sql.Date = null
    var health: List[PersonWithHealthDataHealth] = Nil
  }
  class PersonWithHealthDataHealth extends Dto {
    var manipulation_date: java.sql.Date = null
    var vaccine: String = null
    var had_virus: jBoolean = null
  }
  class PersonSimple extends DtoWithId {
    var id: java.lang.Long = null
    var name: String = null
    var sex: String = null
    var birthdate: java.sql.Date = null
  }

  val viewNameToClass = Map[String, Class[_ <: Dto]](
    "person" -> classOf[Person],
    "person_accounts" -> classOf[PersonAccounts],
    "person_accounts_details" -> classOf[PersonAccountsDetails],
    "payment" -> classOf[Payment],
    "person_list" -> classOf[PersonList],
    "person_with_main_account" -> classOf[PersonWithMainAccount],
    "person_health" -> classOf[PersonHealth],
    "person_with_health_data" -> classOf[PersonWithHealthData],
    "person_with_health_data_health" -> classOf[PersonWithHealthDataHealth],
    "person_simple" -> classOf[PersonSimple],
  )
}

class QuereaseActionsSpecs extends AsyncFlatSpec with Matchers with TestQuereaseInitializer with AsyncFlatSpecLike {

  import AppMetadata._

  implicit protected var tresqlResourcesFactory: ResourcesFactory = _
  implicit val fs: FileStreamer = null
  implicit val as: ActorSystem = ActorSystem("querease-action-specs")
  implicit val reqCtx: RequestContext = null

  override def beforeAll(): Unit = {
    querease = new TestQuerease("/querease-action-specs-metadata.yaml") {
      override lazy val viewNameToClassMap = QuereaseActionsDtos.viewNameToClass
    }
    qio = new AppQuereaseIo[Dto](querease)
    super.beforeAll()
    // get rid from thread local resources
    val tresqlResources = tresqlThreadLocalResources.withConn(tresqlThreadLocalResources.conn)
      .withExtraResources(
        tresqlThreadLocalResources.extraResources +
          (querease.defaultCpName -> tresqlThreadLocalResources.withConn(tresqlThreadLocalResources.conn))
      )
    tresqlResourcesFactory = ResourcesFactory(null, null)(tresqlResources)
  }

  behavior of "metadata"

  it should "have correct data" in {
    val pVd = querease.viewDef("person")
    pVd.actions("save").steps(2).isInstanceOf[Action.Validations] should be (true)
    pVd.actions("save").steps(2).asInstanceOf[Action.Validations].validations.head should be {
      "build cursors"
    }
  }

  it should "correctly encode, decode action data" in {
    import io.bullet.borer._
    import CacheIo.actionCodec
    val actionData: Map[(String, String), AppMetadata.Action] =
      querease.nameToViewDef.flatMap { case (vn, vd) =>
        vd.actions.map { case (n, a) => ((n, vn), a) }.toList
      }
    import org.scalatest.Inspectors._
    convertAssertionToFutureAssertion(
      forAll(actionData) {
        case ((an, vn), a) =>
          val enc_a = Cbor.encode(a).toByteArray
          ((an, vn), a) shouldBe ((an, vn), Cbor.decode(enc_a).to[AppMetadata.Action].value)
      }
    )
  }

  behavior of "person save action"
  import QuereaseActionsDtos._
  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  it should "fail account count validation" in {
    val p = new Person
    val pa = List(new PersonAccounts, new PersonAccounts, new PersonAccounts, new PersonAccounts)
    p.accounts = pa
    recoverToExceptionIf[ValidationException] {
      querease.doAction("person", "save", p.toMap(querease), Map())
    }.map(_.details should be(List(ValidationResult(Nil,
      List(
        "person cannot have more than 3 accounts, got '4'",
        "person cannot have more than 3 accounts, got '4' with total balance (0.00)",
        "person cannot have more than 3 accounts, instead '4' encountered"
      )
    )))).flatMap { _ =>
      p.accounts = Nil
      recoverToExceptionIf[ValidationException] {
        querease.doAction("person", "save", p.toMap(querease), Map())
      }.map(_.details should be(List(ValidationResult(Nil,
        List("person must have at least one account")
      ))))
    }
  }

  it should "fail balance validation" in {
    val p = new Person
    val pa = new PersonAccounts
    pa.number = "AAA"
    pa.balance = 10
    p.accounts = List(new PersonAccounts, new PersonAccounts, pa)
    recoverToExceptionIf[ValidationException] {
      querease.doAction("person", "save", p.toMap(querease), Map())
    }.map(_.details should be(List(ValidationResult(Nil,
      List("Wrong balance for accounts 'AAA(10.00 != 0.00)'")
    )))).flatMap { _ =>
      val pa1 = new PersonAccounts
      pa1.number = "BBB"
      pa1.balance = 2
      p.accounts = List(new PersonAccounts, pa, pa1)
      recoverToExceptionIf[ValidationException] {
        querease.doAction("person", "save", p.toMap(querease), Map())
      }.map(_.details should be(List(ValidationResult(Nil,
        List("Wrong balance for accounts 'AAA(10.00 != 0.00),BBB(2.00 != 0.00)'")
      ))))
    }
  }

  it should "return person" in {
    val p = new Person
    p.name = "Kalis"
    p.surname = "Calis"
    p.sex = "M"
    p.birthdate = java.sql.Date.valueOf("1980-12-14")
    val pa = new PersonAccounts
    pa.number = "AAA"
    pa.balance = 0
    pa.last_modified = java.sql.Timestamp.valueOf("2021-06-17 17:16:00")
    p.accounts = List(pa)
    querease.doAction("person", "save", p.toMap(querease), Map()).map {
      case CompatibleResult(r: TresqlSingleRowResult, _, _) =>
        removeIds(r.map(querease.toCompatibleMap(_, querease.viewDef("person")))) should be {
        Map("name" -> "Mr. Kalis", "surname" -> "Calis", "sex" -> "M",
          "birthdate" -> java.sql.Date.valueOf("1980-12-14"), "main_account" -> null, "accounts" ->
            List(Map("number" -> "AAA", "balance" -> 0.00,
              "last_modified" -> java.sql.Timestamp.valueOf("2021-06-17 17:16:00.0"))))
      }
      case x => sys.error("Unexpected action result class: " + Option(x).map(_.getClass.getName).orNull)
    }.flatMap { _ =>
      p.name = "Zina"
      p.surname = "Mina"
      p.sex = "F"
      p.birthdate = java.sql.Date.valueOf("1982-12-14")
      pa.number = "BBB"
      pa.balance = 0
      pa.last_modified = java.sql.Timestamp.valueOf("2021-06-19 00:15:00")
      p.accounts = List(pa)
      querease.doAction("person", "save", p.toMap(querease), Map()).map {
        case CompatibleResult(r: TresqlSingleRowResult, _, _) =>
          removeIds(r.map(querease.toCompatibleMap(_, querease.viewDef("person")))) should be {
          Map("main_account" -> null, "name" -> "Ms. Zina", "surname" -> "Mina", "sex" -> "F",
            "birthdate" -> java.sql.Date.valueOf("1982-12-14"), "accounts" ->
              List(Map("number" -> "BBB", "balance" -> 0.00,
                "last_modified" -> java.sql.Timestamp.valueOf("2021-06-19 00:15:00.0"))))
        }
        case x => sys.error("Unexpected action result class: " + Option(x).map(_.getClass.getName).orNull)
      }
    }
  }

  behavior of "payment save action"

  it should "fail amount validation" in {
    val p = new Payment
    p.amount = 0
    p.beneficiary = "AAA"
    recoverToExceptionIf[ValidationException] {
      querease.doAction("payment", "save", p.toMap(querease), Map())
    }.map(_.details should be(List(ValidationResult(Nil,
      List("Wrong amount 0. Amount must be greater than 0")
    )))).flatMap { _ =>
      p.originator = "BBB"
      p.amount = 10
      recoverToExceptionIf[ValidationException] {
        querease.doAction("payment", "save", p.toMap(querease), Map())
      }.map(_.details should be(List(ValidationResult(Nil,
        List("Insufficient funds for account 'BBB'")
      ))))
    }
  }

  it should "register payments" in {
    val p = new Payment
    p.amount = 10
    p.beneficiary = "AAA"
    querease.doAction("payment", "save", p.toMap(querease), Map()).flatMap { _ =>
      p.originator = "AAA"
      p.beneficiary = "BBB"
      p.amount = 2
      querease.doAction("payment", "save", p.toMap(querease), Map()).map { res =>
        res.getClass.getName should be ("org.wabase.TresqlResult")
      }
    }.map { _ =>
      implicit val res = tresqlResourcesFactory.resources
      Query("account{number, balance}#(1)").toListOfMaps should be(
        List(Map("number" -> "AAA", "balance" -> 8.00), Map("number" -> "BBB", "balance" -> 2.00)))
    }
  }

  behavior of "person list"

  it should "return person list with count" in {
    querease.doAction("person_list", "list", Map(), Map("sort" -> "~name")).map {
      case MapResult(res) => removeIds(res) should be (
        Map("count" -> 2, "data" ->
          List(Map("name" -> "Ms. Zina", "surname" -> "Mina", "sex" -> "F", "birthdate" -> java.sql.Date.valueOf("1982-12-14")),
            Map("name" -> "Mr. Kalis", "surname" -> "Calis", "sex" -> "M", "birthdate" -> java.sql.Date.valueOf("1980-12-14"))))
      )
      case x => sys.error("Unexpected action result class: " + Option(x).map(_.getClass.getName).orNull)
    }.flatMap { _ =>
      querease.doAction("person_list", "list", Map("name" -> "Ms", "sort" -> "name"), Map()).map {
        case MapResult(res) => removeIds(res) should be (
          Map("count" -> 1, "data" ->
            List(Map("name" -> "Ms. Zina", "surname" -> "Mina", "sex" -> "F", "birthdate" -> java.sql.Date.valueOf("1982-12-14")))
          )
        )
        case x => sys.error("Unexpected action result class: " + Option(x).map(_.getClass.getName).orNull)
      }
    }.flatMap { _ =>
      querease.doAction("person_list", "list", Map("name" -> "Ms"), Map("sort" -> "name")).map {
        case MapResult(res) => removeIds(res) should be (
          Map("count" -> 1, "data" ->
            List(Map("name" -> "Ms. Zina", "surname" -> "Mina", "sex" -> "F", "birthdate" -> java.sql.Date.valueOf("1982-12-14"))))
        )
        case x => sys.error("Unexpected action result class: " + Option(x).map(_.getClass.getName).orNull)
      }
    }
  }

  behavior of "person with main account"

  it should "return person with main account" in {
    implicit val res = tresqlResourcesFactory.resources
    val name = "Kalis"
    val id = Query("person[name %~~% ?] {id}", name).unique[Long]
    querease.doAction("person_with_main_account", "get", Map("id" -> id), Map()).map {
      case MapResult(res) => removeKeys(res, Set("id", "last_modified")) should be (Map(
        "main_account" -> "<no main account>",
        "name" -> "Mr. Kalis",
        "surname" -> "Calis",
        "sex" -> "M",
        "birthdate" -> java.sql.Date.valueOf("1980-12-14"),
        "accounts" -> List(Map("number" -> "AAA", "balance" -> 8.00))))
      case x => sys.error("Unexpected action result class: " + Option(x).map(_.getClass.getName).orNull)
    }.flatMap { _ =>
      //set main account
      Query("=person[id = ?] {main_account_id = account[number = 'AAA' & person_id = ?]{id}}", id, id)
      querease.doAction("person_with_main_account", "get", Map("id" -> id), Map()).map {
        case MapResult(res) => removeKeys(res, Set("id", "last_modified")) should be (Map(
          "main_account" -> "AAA(8.00)",
          "name" -> "Mr. Kalis",
          "surname" -> "Calis",
          "sex" -> "M",
          "birthdate" -> java.sql.Date.valueOf("1980-12-14"),
          "accounts" -> List(Map("number" -> "AAA", "balance" -> 8.00))))
        case x => sys.error("Unexpected action result class: " + Option(x).map(_.getClass.getName).orNull)
      }
    }
  }

  behavior of "variable transformations"

  it should "transform variables" in {
    querease.doAction("variable_transform_test", "get", Map(), Map()).map {
      _ shouldBe MapResult(Map("name" -> "Gunzis", "job" -> "Developer"))
    }
  }

  behavior of "extra db support"

  it should "fail to register non existing person health data" in {
    val ph = new PersonHealth
    ph.name = "Gunza"
    ph.vaccine = "AstraZeneca"
    ph.had_virus = null
    ph.manipulation_date = java.sql.Date.valueOf("2021-06-05")
    recoverToExceptionIf[ValidationException] {
      querease.doAction("person_health", "save", ph.toMap(querease), Map())
    }.map(_.details should be (List(ValidationResult(Nil, List("Person 'Gunza' must be registered")))))

    val m =
      Map("current_person" -> "Gunzagi", "vaccine" -> "AstraZeneca", "manipulation_date" -> java.sql.Date.valueOf("2021-06-05"))
    recoverToExceptionIf[ValidationException] {
      querease.doAction("person_health_priv", "save", m, Map())
    }.map(_.details should be (List(ValidationResult(Nil, List("Person 'Gunzagi' must be registered")))))
  }

  it should "register person health data" in {
    implicit val res = tresqlResourcesFactory.resources

    val persons = List(
      Map("name" -> "Mario", "sex" -> "M", "birthdate" -> java.sql.Date.valueOf("1988-09-12")),
      Map("name" -> "Gunzagi", "sex" -> "M", "birthdate" -> java.sql.Date.valueOf("1999-06-23")),
    )
    val vaccines = List(
      Map("name" -> "Mario", "vaccine" -> "Pfizer", "manipulation_date" -> java.sql.Date.valueOf("2021-08-10")),
    )
    val vaccines_priv = List(
      Map("current_person" -> "Gunzagi", "vaccine" -> "AstraZeneca", "manipulation_date" -> java.sql.Date.valueOf("2021-06-05")),
    )
    def saveData(view: String, data: List[Map[String, Any]])(implicit res: Resources) =
      data.foldLeft(Future.successful[QuereaseResult](LongResult(0))) { (r, d) =>
        r.flatMap(_ => querease.doAction(view, "save", d, Map())(
          tresqlResourcesFactory, implicitly[ExecutionContext], as, fs, reqCtx, qio))
      }

    saveData("person_simple", persons)
      .flatMap(_ => saveData("person_health", vaccines))
      .flatMap(_ => saveData("person_health_priv", vaccines_priv))
      .flatMap { _ =>
        querease.doAction("person_with_health_data", "list", Map("names" -> List("Mario", "Gunzagi")), Map()).map {
          case CompatibleResult(TresqlResult(res), _, _) =>
            res.toListOfMaps.map(m => (new PersonWithHealthData).fill(m)(querease).toMap(querease)).toList should be (
              List(
                Map("name" -> "Gunzagi", "sex" -> "M", "birthdate" -> java.sql.Date.valueOf("1999-06-23"),
                  "health" ->
                    List(
                      Map("manipulation_date" -> java.sql.Date.valueOf("2021-06-05"), "vaccine" -> "AstraZeneca", "had_virus" -> null)
                    )
                ),
                Map("name" -> "Mario", "sex" -> "M", "birthdate" -> java.sql.Date.valueOf("1988-09-12"), "health" ->
                  List(Map("manipulation_date" -> java.sql.Date.valueOf("2021-08-10"), "vaccine" -> "Pfizer", "had_virus" -> null))
                )
              )
            )
          case x => fail(s"Unexpected result: $x")
        }
      }
  }

  it should "switch db context when calling action on another view" in {
    recoverToExceptionIf[ValidationException] {
      querease.doAction("db_context_person", "update", Map("id" -> 0), Map())
    }.map(_.details should be (List(ValidationResult(Nil, List("Person health record to be updated must exist")))))
  }


  behavior of "config"

  it should "process config" in {
    querease.doAction("conf_test", "get", Map(), Map()).map {
      case r => r should be (StatusResult(200, StringStatus("http://wabase.org/about")))
    }.flatMap { _ =>
      querease.doAction("conf_test", "list", Map(), Map()).map {
        case r => r should be(
          ConfResult("conf.test", Map("uri" -> "http://wabase.org/", "list" -> List(1, 2, 3))))
      }
    }
  }

  behavior of "escape syntax"

  it should "use tresql instead of view call - escape syntax" in {
    querease.doAction("escape_syntax", "insert", Map("key" -> "k", "value" -> "v"), Map())
      .flatMap { _ =>
        querease.doAction("escape_syntax", "list", Map(), Map())
      }
      .mapTo[TresqlResult]
      .map {
        _.result.toListOfMaps shouldBe List(Map("key" -> "k", "value" -> "v"))
      }
  }
}

class QuereaseActionTestManager extends Loggable {
  def personSaveBizMethod(data: Map[String, Any]) = {
    if (data("sex") == "F")
      data + ("name" -> s"Ms. ${data("name")}")
    else data
  }

  def personSaveDtoBizMethod(data: Person): Person = {
    if (data.sex == "M") {
      data.name = s"Mr. ${data.name}"
      data
    }
    else data
  }

  def personSaveJavaMapBizMethod(data: java.util.Map[String, Any]) = {
    data
  }

  def sendNotifications(data: Map[String, Any]): Unit = {
    logger.info("Person data change notifications sender called")
  }

  def concatStrings(data: Map[String, Any]): String = {
    data.getOrElse("s1", "").toString + " " + data.getOrElse("s2", "").toString
  }

  def addNumbers(data: Map[String, Any]): java.lang.Number = {
    BigDecimal(data("n1").toString) + BigDecimal(data("n2").toString)
  }

  def ambiguousMethod(data: Map[String, Any]) = {}
  def ambiguousMethod(data: java.util.Map[String, Any]) = {}
  def unitMethod(): Unit = {}
  def unitFutureMethod(): Unit = Future.successful(())
  def httpReqMethod(req: HttpRequest, data: Map[String, Any]) =
    if (req == null) s"${data.size}" else s"${req.uri.toString} = ${data.size}"

  def rowLikeMethod(data: Map[String, Any], res: Resources) = {
    resultMethod(data)(res).toList.head // do not use result's unique method since it returns itself and in querease will match Result not RowLike
  }
  def resultMethod(data: Map[String, Any])(implicit res: Resources) = {
    val tresql = data.flatMap { case (k, v) =>
      v match { case s: String => List(s"'$s' '$k'") case _ => Nil }
    }.mkString("{ ", ", ", " }")
    Query(tresql)
  }
  def iteratorMethod(data: Map[String, Any]) = {
    List(data - "x").iterator
  }
  def seqMethod(data: Map[String, Any]) = {
    List(data - "x")
  }
}

class QuereaseActionTestManagerObj {
  def unitMethod(): Unit = {}
}

object QuereaseActionTestManagerObj {
  def unitMethod(): Unit = new QuereaseActionTestManagerObj().unitMethod()

  def name_surname_formatter(res: TresqlResult) = {
    val tr = res.result.map(row => Map("person_name" -> s"${row("name")} ${row("surname")}"))
    IteratorResult(tr)
  }
  def result_render_test() = Map[String, Any](
      "string_field" -> "string",
      "extra field" -> 1,
    )
  def free_result_render_test() = Map[String, Any](
    "1" -> Map("key" -> "value"),
  )
  def free_result_render_list_test() = List[Map[String, Any]](
    Map("1" -> List(Map("key1" -> "value1"))),
    Map("2" -> List(Map("key2" -> "value2"))),
  )
  def int_array() = Array(1 ,2, 3)
  def person_dtos_list() = {
    import org.wabase.QuereaseActionsDtos._
    def pers(id: Long, name: String, sex: String, birthdate: java.sql.Date) = {
      val p = new PersonSimple
      p.id = id
      p.name = name
      p.sex = sex
      p.birthdate = birthdate
      p
    }
    List(
      pers(1, "Kizis", "M", java.sql.Date.valueOf("1977-04-10")),
      pers(2, "Ala", "F", java.sql.Date.valueOf("1955-07-01")),
      pers(3, "Ola", "F", java.sql.Date.valueOf("1988-10-09")),
    )
  }
  def businessException(data: Map[String, Any]) = {
    throw new BusinessException("Invocation error")
  }
  def processRequestParts(res: RequestPartResult)(implicit as: ActorSystem, ec: ExecutionContext) =
    res.result.mapAsync(1) { part =>
      part.data.runWith(AppFileStreamer.sha256sink).map(sha => Map("file" -> part.filename, "sha_256" -> sha))
    }.runFold(List[Map[String, Any]]())(_ :+ _)
}
