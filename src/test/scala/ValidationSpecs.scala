package org.wabase

import org.mojoz.querease.{ValidationException, ValidationResult}

class ValidationSpecsQuerease extends QuereaseBase("/validation-specs-metadata.yaml") {
  import ValidationSpecsQuerease._
  override lazy val viewNameToClassMap = Map[String, Class[_ <: Dto]](
    "validations_test" -> classOf[validations_test],
    "validations_test" -> classOf[validations_test],
    "validations_test" -> classOf[validations_test],
  )
}
object ValidationSpecsQuerease extends ValidationSpecsQuerease {
  class validations_test extends DtoWithId {
    var id: java.lang.Long = null
    var int_col: java.lang.Integer = null
    var children1: List[validations_test_child1] = Nil
    var children2: List[validations_test_child2] = Nil
  }
  class validations_test_child extends DtoWithId {
    var id: java.lang.Long = null
    var int_col: java.lang.Integer = null
  }
  class validations_test_child1 extends validations_test_child
  class validations_test_child2 extends validations_test_child
}

class ValidationSpecsApp(val validationsDbAccess: DbAccess) extends AppBase[TestUsr] with NoAudit[TestUsr] with PostgreSqlConstraintMessage with
  DbAccessDelegate with NoAuthorization[TestUsr] with NoValidation /* ha ha yeah */ {
  override type QE = ValidationSpecsQuerease
  override protected def initQuerease: QE = ValidationSpecsQuerease
  override def dbAccessDelegate: DbAccess = validationsDbAccess
}

class ValidationSpecs extends QuereaseBaseSpecs {

  import ValidationSpecsQuerease._

  var testApp: ValidationSpecsApp = _

  override def beforeAll(): Unit = {
    querease = ValidationSpecsQuerease
    super.beforeAll()
    val db = new DbAccess with Loggable {
      override val tresqlResources = ValidationSpecs.this.tresqlResources

      override def dbUse[A](a: => A)(implicit timeout: QueryTimeout, pool: PoolName): A =
        try a finally tresqlResources.conn.rollback
      override protected def transactionInternal[A](forceNewConnection: Boolean, a: => A)(implicit timeout: QueryTimeout,
                                                                                          pool: PoolName): A =
        try a finally tresqlResources.conn.commit
    }
    testApp = new ValidationSpecsApp(db)
  }

  implicit val usr = TestUsr(1)
  implicit val state: ApplicationState = ApplicationState(Map())
  implicit val timeout: QueryTimeout = DefaultQueryTimeout.get

  it should "validate on save" in {
    val dto = new validations_test

    dto.int_col = 3
    intercept[ValidationException] {
      testApp.saveDto(dto)
    }.details should be(List(ValidationResult(Nil,
      List("int_col should be greater than 5 but is 3", "int_col should be greater than 10 but is 3")
    )))

    dto.int_col = 7
    intercept[ValidationException] {
      testApp.saveDto(dto)
    }.details should be(List(ValidationResult(Nil,
      List("int_col should be greater than 10 but is 7")
    )))

    dto.int_col = 11
    testApp.saveDto(dto) should be(0)

    dto.int_col = 13
    intercept[ValidationException] {
      testApp.saveDto(dto)
    }.details should be(List(ValidationResult(Nil,
      List("int_col should be less than 12 but is 13")
    )))

    dto.int_col = 11
    val ch11 = new validations_test_child1
    ch11.int_col = 0
    val ch12 = new validations_test_child1
    ch12.int_col = 1
    val ch21 = new validations_test_child2
    ch21.int_col = 0
    val ch22 = new validations_test_child2
    ch22.int_col = 1
    dto.children1 = List(ch11, ch12)
    dto.children2 = List(ch21, ch22)
    intercept[ValidationException] {
      testApp.saveDto(dto)
    }.details should be(
      List(ValidationResult(List("children1", 0), List("child1 int_col should be greater than 1 but is 0")),
        ValidationResult(List("children1", 1), List("child1 int_col should be greater than 1 but is 1")),
        ValidationResult(List("children2", 0), List("child2 int_col should be greater than 2 and parent must be greater than 3 but is 0,11")),
        ValidationResult(List("children2", 1), List("child2 int_col should be greater than 2 and parent must be greater than 3 but is 1,11")))
    )

    dto.int_col = 0
    intercept[ValidationException] {
      testApp.saveDto(dto)
    }.details should be(
      List(ValidationResult(Nil, List("int_col should be greater than 5 but is 0", "int_col should be greater than 10 but is 0")),
        ValidationResult(List("children1", 0), List("child1 int_col should be greater than 1 but is 0")),
        ValidationResult(List("children1", 1), List("child1 int_col should be greater than 1 but is 1")),
        ValidationResult(List("children2", 0), List("child2 int_col should be greater than 2 and parent must be greater than 3 but is 0,0")),
        ValidationResult(List("children2", 1), List("child2 int_col should be greater than 2 and parent must be greater than 3 but is 1,0")))
    )

    dto.int_col = 11
    dto.children1(0).int_col = 2
    dto.children1(1).int_col = 2
    dto.children2(0).int_col = 3
    dto.children2(1).int_col = 3
    testApp.saveDto(dto) should be(0)

    dto.int_col = 11
    dto.children1(0).int_col = 1
    dto.children2(1).int_col = 2
    intercept[ValidationException] {
      testApp.saveDto(dto)
    }.details should be(
      List(ValidationResult(List("children1", 0), List("child1 int_col should be greater than 1 but is 1")),
        ValidationResult(List("children2", 1), List("child2 int_col should be greater than 2 and parent must be greater than 3 but is 2,11")))
    )
  }
}
