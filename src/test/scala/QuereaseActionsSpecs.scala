package org.wabase

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.HttpRequest
import org.apache.pekko.stream.scaladsl.StreamConverters
import org.mojoz.querease.{ValidationException, ValidationResult}
import org.scalatest.flatspec.{AsyncFlatSpec, AsyncFlatSpecLike}
import org.scalatest.matchers.should.Matchers
import org.tresql.{Query, Resources, Result, SingleValueResult, convAny, convLong}
import org.wabase.QuereaseActionsDtos.{Person, PersonWithHealthDataHealth}

import java.io.InputStream
import scala.concurrent.duration.DurationInt
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
    var beneficiary_name: String = null
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

@annotation.nowarn("msg=Manifest")
class QuereaseActionsSpecs extends AsyncFlatSpec with Matchers with TestQuereaseInitializer with AsyncFlatSpecLike {

  import AppMetadata._

  implicit var qr: QuereaseResources = null

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
    qr = new QuereaseResources()(ResourcesFactory(null, null)(tresqlResources),
      scala.concurrent.ExecutionContext.global, ActorSystem("querease-action-specs"), null, qio,
      WabaseFileStreamers(Map("main" -> null)), null, _ => PartialFunction.empty, logger)
  }

  def doAction(view: String, action: String, data: Map[String, Any], env: Map[String, Any]) = {
    querease.doAction(view, action, data, env).transform(identity, {
      case e: QuereaseActionException => e.getCause case e => e
    })
  }

  behavior of "metadata"

  it should "have correct data" in {
    val pVd = querease.viewDef("person")
    pVd.actions("save").steps(2)._1.isInstanceOf[Action.Validations] should be (true)
    pVd.actions("save").steps(2)._1.asInstanceOf[Action.Validations].validations.head should be {
      "build cursors"
    }
  }

  it should "parse try and recover blocks" in {
    val vd = querease.viewDef("try_test_1")
    def tryOp(actionName: String): Action.Try = vd.actions(actionName).steps match {
      case (Action.Evaluation(None, Nil, t: Action.Try), _) :: Nil => t
      case x => fail(s"Unexpected steps of action '$actionName': $x")
    }
    def srcs(a: AppMetadata.Action) = a.steps.map(_._2)
    // try block op, recover block op
    srcs(tryOp("get").action)         should be (List("x = 'T'", ":x"))
    srcs(tryOp("get").recoverAct)     should be (List("x = 'R'", ":x"))
    // try op, recover block op
    srcs(tryOp("insert").action)      should be (List("'T'"))
    srcs(tryOp("insert").recoverAct)  should be (List("x = 'R'", ":x"))
    // try block op, recover op
    srcs(tryOp("update").action)      should be (List("x = 'T'", ":x"))
    srcs(tryOp("update").recoverAct)  should be (List("'R'"))
    // try op, recover block op on the same step
    srcs(tryOp("delete").action)      should be (List("'T'"))
    srcs(tryOp("delete").recoverAct)  should be (List("x = 'R'", ":x"))
    // try op, recover op
    srcs(tryOp("count").action)       should be (List("'T'"))
    srcs(tryOp("count").recoverAct)   should be (List("'R'"))
    // try op without recover
    srcs(tryOp("list").action)        should be (List("'T'"))
    tryOp("list").recoverAct          should be (null)
  }

  it should "do raw action json encoding" in {
    import org.apache.pekko.util.ByteString
    import org.scalatest.Inspectors._
    val viewActionData = querease.viewDefLoader.nameToViewDef.flatMap { case (_, vd) =>
      Action().map { actionName => (actionName, vd) }
    }
    convertAssertionToFutureAssertion(
      forAll(viewActionData) { case (actionName, vd) =>
        val seq     = ViewDefExtrasUtils.getSeq(actionName, vd.extras)
        val encoded = ResultEncoder.encodeAnyToJsonBytes(seq)
        val decoded = CborOrJsonAnyValueDecoder.decode(ByteString(encoded))
        ResultEncoder.encodeAnyToJsonBytes(decoded) shouldBe encoded
      }
    )
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

  behavior of "constants"

  it should "return string constant" in {
    doAction("constants", "get", Map(), Map())
      .mapTo[TresqlResult]
      .flatMap(_.result.unique[Any] shouldBe "text")
  }

  it should "return integer constant" in {
    doAction("constants", "insert", Map(), Map())
      .mapTo[TresqlResult]
      .flatMap(_.result.unique[Any] shouldBe 10)
  }

  it should "return decimal constant" in {
    doAction("constants", "update", Map(), Map())
      .mapTo[TresqlResult]
      .flatMap(_.result.unique[Any] shouldBe 1.5)
  }

  it should "return boolean constant" in {
    doAction("constants", "delete", Map(), Map())
      .mapTo[TresqlResult]
      .flatMap(_.result.unique[Any] shouldBe true)
  }

  it should "return null constant" in {
    doAction("constants", "list", Map(), Map())
      .mapTo[TresqlResult]
      .flatMap(_.result.unique[Any] shouldBe (null :String))
  }

  behavior of "person save action"
  import QuereaseActionsDtos._

  it should "fail account count validation" in {
    val p = new Person
    val pa = List(new PersonAccounts, new PersonAccounts, new PersonAccounts, new PersonAccounts)
    p.accounts = pa
    recoverToExceptionIf[ValidationException] {
      doAction("person", "save", p.toMap(querease), Map())
    }.map(_.details should be(List(ValidationResult(Nil,
      List(
        "person cannot have more than 3 accounts, got '4'",
        "person cannot have more than 3 accounts, got '4' with total balance (0.00)",
        "person cannot have more than 3 accounts, instead '4' encountered"
      )
    )))).flatMap { _ =>
      p.accounts = Nil
      recoverToExceptionIf[ValidationException] {
        doAction("person", "save", p.toMap(querease), Map())
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
      doAction("person", "save", p.toMap(querease), Map())
    }.map(_.details should be(List(ValidationResult(Nil,
      List("Wrong balance for accounts 'AAA(10.00 != 0.00)'")
    )))).flatMap { _ =>
      val pa1 = new PersonAccounts
      pa1.number = "BBB"
      pa1.balance = 2
      p.accounts = List(new PersonAccounts, pa, pa1)
      recoverToExceptionIf[ValidationException] {
        doAction("person", "save", p.toMap(querease), Map())
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
    doAction("person", "save", p.toMap(querease), Map()).map {
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
      doAction("person", "save", p.toMap(querease), Map()).map {
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
    p.beneficiary_name = "Mr. Kalis Calis"
    p.beneficiary = "AAA"
    recoverToExceptionIf[ValidationException] {
      doAction("payment", "save", p.toMap(querease), Map())
    }.map(_.details should be(List(ValidationResult(List("amount"),
      List("Wrong amount 0. Amount must be greater than 0")
    )))).flatMap { _ =>
      p.originator = "BBB"
      p.amount = 10
      recoverToExceptionIf[ValidationException] {
        doAction("payment", "save", p.toMap(querease), Map())
      }.map(_.details should be(List(ValidationResult(List("balance"),
        List("Insufficient funds for account 'BBB'")
      ))))
    }
  }

  it should "register payments" in {
    val p = new Payment
    p.amount = 10
    p.beneficiary_name = "Mr. Kalis Calis"
    p.beneficiary = "AAA"
    doAction("payment", "save", p.toMap(querease), Map()).flatMap { _ =>
      p.originator = "AAA"
      p.beneficiary = "BBB"
      p.amount = 2
      doAction("payment", "save", p.toMap(querease), Map()).map { res =>
        res.getClass.getName should be ("org.wabase.TresqlResult")
      }
    }.map { _ =>
      implicit val res = qr.resourcesFactory.resources
      Query("account{number, balance}#(1)").toListOfMaps should be(
        List(Map("number" -> "AAA", "balance" -> 8.00), Map("number" -> "BBB", "balance" -> 2.00)))
    }
  }

  behavior of "person list"

  it should "return person list with count" in {
    doAction("person_list", "list", Map(), Map("sort" -> "~name")).map {
      case MapResult(res) => removeIds(res) should be (
        Map("count" -> 2, "data" ->
          List(Map("name" -> "Ms. Zina", "surname" -> "Mina", "sex" -> "F", "birthdate" -> java.sql.Date.valueOf("1982-12-14")),
            Map("name" -> "Mr. Kalis", "surname" -> "Calis", "sex" -> "M", "birthdate" -> java.sql.Date.valueOf("1980-12-14"))))
      )
      case x => sys.error("Unexpected action result class: " + Option(x).map(_.getClass.getName).orNull)
    }.flatMap { _ =>
      doAction("person_list", "list", Map("name" -> "Ms", "sort" -> "name"), Map()).map {
        case MapResult(res) => removeIds(res) should be (
          Map("count" -> 1, "data" ->
            List(Map("name" -> "Ms. Zina", "surname" -> "Mina", "sex" -> "F", "birthdate" -> java.sql.Date.valueOf("1982-12-14")))
          )
        )
        case x => sys.error("Unexpected action result class: " + Option(x).map(_.getClass.getName).orNull)
      }
    }.flatMap { _ =>
      doAction("person_list", "list", Map("name" -> "Ms"), Map("sort" -> "name")).map {
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
    implicit val res = qr.resourcesFactory.resources
    val name = "Kalis"
    val id = Query("person[name %~~% ?] {id}", name).unique[Long]
    doAction("person_with_main_account", "get", Map("id" -> id), Map()).map {
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
      doAction("person_with_main_account", "get", Map("id" -> id), Map()).map {
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
    for {
      t1 <- doAction("variable_transform_test", "get", Map(), Map()).map {
        _ shouldBe MapResult(Map("name" -> "Gunzis", "job" -> "Developer"))
      }
      t2 <- doAction("variable_transform_test", "insert", Map(), Map())
        .mapTo[TresqlResult]
        .map(_.result)
        .mapTo[SingleValueResult[_]]
        .map(_.value)
        .map {
          _ shouldBe Seq(1, 2, 3, 3, 4, 5)
        }
      t3 <- doAction("variable_transform_test", "update",
        Map("upd" -> true, "name" -> "Joe", "surname" -> "Doe"), Map()
      )
        .mapTo[TresqlResult]
        .map(_.result.toListOfVectors.head.head)
        .map {
          _ shouldBe "Joe Doe"
        }
      t4 <- doAction("variable_transform_test", "update",
        Map("upd" -> false, "name" -> "Joe", "surname" -> "Doe"), Map()
      )
        .mapTo[TresqlResult]
        .map(_.result.toListOfVectors.head.head)
        .map {
          _ shouldBe "Joe"
        }
      t5 <- doAction("variable_transform_test", "delete", Map("a" -> "v", "b" -> List(1, 2)), Map())
        .mapTo[TresqlResult]
        .map(_.result)
        .mapTo[SingleValueResult[_]]
        .map(_.value)
        .map {
          _ shouldBe Map("header" -> "header value", "body" -> Map("a" -> "v", "b" -> List(1, 2)))
        }
    } yield t1
  }

  behavior of "extra db support"

  it should "fail to register non existing person health data" in {
    val ph = new PersonHealth
    ph.name = "Gunza"
    ph.vaccine = "AstraZeneca"
    ph.had_virus = null
    ph.manipulation_date = java.sql.Date.valueOf("2021-06-05")
    recoverToExceptionIf[ValidationException] {
      doAction("person_health", "save", ph.toMap(querease), Map())
    }.map(_.details should be (List(ValidationResult(Nil, List("Person 'Gunza' must be registered")))))

    val m =
      Map("current_person" -> "Gunzagi", "vaccine" -> "AstraZeneca", "manipulation_date" -> java.sql.Date.valueOf("2021-06-05"))
    recoverToExceptionIf[ValidationException] {
      doAction("person_health_priv", "save", m, Map())
    }.map(_.details should be (List(ValidationResult(List("check_person"), List("Person 'Gunzagi' must be registered")))))
  }

  it should "register person health data" in {
    implicit val res = qr.resourcesFactory.resources

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
        r.flatMap(_ => doAction(view, "save", d, Map()))
      }

    saveData("person_simple", persons)
      .flatMap(_ => saveData("person_health", vaccines))
      .flatMap(_ => saveData("person_health_priv", vaccines_priv))
      .flatMap { _ =>
        doAction("person_with_health_data", "list", Map("names" -> List("Mario", "Gunzagi")), Map()).map {
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
      doAction("db_context_person", "update", Map("id" -> 0), Map())
    }.map(_.details should be (List(ValidationResult(List("check_health_exists"), List("Person health record to be updated must exist")))))
  }


  behavior of "config"

  it should "process config" in {
    doAction("conf_test", "get", Map(), Map()).map {
      case r => r should be (ResponseResult(200, ResultValue(StringResult("http://wabase.org/about"))))
    }.flatMap { _ =>
      doAction("conf_test", "list", Map(), Map()).map {
        case r => r should be(
          ConfResult("conf.test", Map(
            "uri" -> "http://wabase.org/",
            "list" -> List(1, 2, 3),
            "enabled" -> true,
            "request-timeout" -> 10,
          )))
      }
    }
  }

  behavior of "escape syntax"

  it should "use tresql instead of view call - escape syntax" in {
    doAction("escape_syntax", "insert", Map("key" -> "k", "value" -> "v"), Map())
      .flatMap { _ =>
        doAction("escape_syntax", "list", Map(), Map())
      }
      .mapTo[TresqlResult]
      .map {
        _.result.toListOfMaps shouldBe List(Map("key" -> "k", "value" -> "v"))
      }
  }

  behavior of "Macros.dynamic_sql"

  it should "filter with literal SQL condition" in {
    doAction("dynamic_sql_test", "get", Map(), Map()).map {
      case MapResult(res) => res should be(Map("name" -> "Mr. Kalis", "surname" -> "Calis"))
      case x => sys.error("Unexpected action result class: " + Option(x).map(_.getClass.getName).orNull)
    }
  }

  it should "filter with SQL condition containing bind variable" in {
    doAction("dynamic_sql_test", "list", Map(), Map()).map {
      case TresqlResult(res) => res.toListOfMaps should be(List(Map("name" -> "Ms. Zina", "surname" -> "Mina")))
      case x => sys.error("Unexpected action result class: " + Option(x).map(_.getClass.getName).orNull)
    }
  }
}

@annotation.nowarn("msg=Manifest")
class QuereaseActionTestPersonManager {
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

  def multipleArgConversions(data: Map[String, Any], dto: PersonWithHealthDataHealth,
                             list: Seq[Map[String, Any]], arr: Array[PersonWithHealthDataHealth],
                            )(implicit qe: AppQuerease, qio: AppQuereaseIo[Dto]): Boolean = {
    qio.fill[PersonWithHealthDataHealth](data).toMap == dto.toMap &&
      list.map(qio.fill[PersonWithHealthDataHealth](_).toMap) == arr.map(_.toMap).toSeq
  }
}

@annotation.nowarn("msg=Manifest")
class QuereaseActionTestManager extends QuereaseActionTestPersonManager with Loggable {

  def sendNotifications(data: Map[String, Any]): Unit = {
    logger.info("Person data change notifications sender called")
  }

  def concatStrings(data: Map[String, Any]): String = {
    if (data != null)
      data.getOrElse("s1", "").toString + " " + data.getOrElse("s2", "").toString
    else null
  }

  def addNumbers(data: Map[String, Any]): java.lang.Number = {
    BigDecimal(data("n1").toString) + BigDecimal(data("n2").toString)
  }

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

  def booleanMethod(b: Boolean): Boolean = b

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
  /* Must throw IllegalArgumentException when called from view action */
  def unsupportedParamMethod(str: String) = str
  def processRequestParts(res: RequestPartResult)(implicit as: ActorSystem, ec: ExecutionContext) =
    res.result.mapAsync(1) { part =>
      part.entity.dataBytes.runWith(AppFileStreamer.sha256sink).map(sha => Map("file" -> part.filename, "sha_256" -> sha))
    }.runFold(List[Map[String, Any]]())(_ :+ _)

  def customDecoder(req: HttpRequest)(implicit as: ActorSystem, ec: ExecutionContext): Future[Map[String, Any]] = {
    req.entity.toStrict(1.second).map(_.data.utf8String).map(_.split("\n").toList).map {
      case List(h, v) => (h.split(",") zip v.split(",")).toMap
      case x => sys.error(s"Illegal argument: $x")
    }
  }
  def httpRequest(data: Map[String, Any], httpClients: WabaseHttpClients) = {
    val httpClient = httpClients.httpClients.head._2
    httpClient(null)(HttpRequest(uri = data("uri").toString))
  }
  def stringArgument(s: String) = s + " " + s
  def multipleStringArguments(s1: String, s2: String, s3: String) = s1 + " " + s2 + " " + s3
  @annotation.nowarn("msg=Manifest")
  def multipleArguments(s1: String, result: TresqlResult, s2: String, vars: Map[String, Any])(implicit res: Resources) = {
    import org.tresql.CoreTypes.convString // scala 3 peculiarity
    s1 + " " + Query(result.result.unique[String], vars).unique[String] + " " + s2
  }
  def httpResult(httpResult: HttpResult) = httpResult
  @annotation.nowarn("msg=Manifest")
  def stringResultFromInputStream(tresqlResult: TresqlResult)(implicit qr: QuereaseResources) = {
    import qr._
    import org.tresql.convInputStream
    StreamConverters
      .fromInputStream(() => tresqlResult.result.unique[InputStream])
      .runReduce(_ ++ _)
      .map(_.utf8String)
  }
  def tresqlResult(res: Result[_]) = res.toListOfMaps
  def listOfMaps(res: Seq[Map[String, Any]]) = res
  @annotation.nowarn("msg=Manifest")
  def stringResultsFromInputStreams(tresqlResult: TresqlResult)(implicit qr: QuereaseResources) = {
    import qr._
    import org.tresql.convInputStream
    Future.traverse(tresqlResult.result.unique[InputStream, InputStream, InputStream].productIterator.map { in =>
      StreamConverters
        .fromInputStream(() => in.asInstanceOf[InputStream])
        .runReduce(_ ++ _)
        .map(_.utf8String)
    })(identity)
  }
}
