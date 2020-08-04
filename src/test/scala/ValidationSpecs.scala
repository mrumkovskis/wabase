package org.wabase

import java.sql.{Connection, DriverManager}

import mojoz.metadata.in.YamlMd
import mojoz.metadata.out.SqlWriter
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers
import org.tresql.dialects
import spray.json._
import spray.json.DefaultJsonProtocol.{StringJsonFormat, jsonFormat2, listFormat}

import scala.reflect.ClassTag

trait ValidationSpecsQuerease extends AppQuerease {
  import ValidationSpecsQuerease._
  override type DTO = org.wabase.Dto
  override type DWI = org.wabase.DtoWithId
  override lazy val yamlMetadata = YamlMd.fromResource("/validation-specs-metadata.yaml")
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

class ValidationSpecs extends FlatSpec with Matchers with BeforeAndAfterAll {

  import ValidationSpecsQuerease._

  var conn: Connection = _

  var testApp: ValidationSpecsApp = _

  val querease: AppQuerease = ValidationSpecsQuerease

  override def beforeAll(): Unit = {
    super.beforeAll()
    Class.forName("org.hsqldb.jdbc.JDBCDriver")
    this.conn = DriverManager.getConnection("jdbc:hsqldb:mem:validation_test")

    val db = new DbAccess with Loggable {
      logger.debug("Creating database for validation tests ...\n")
      SqlWriter.hsqldb().schema(querease.tableMetadata.tableDefs)
        .split(";\\s+").map(_ + ";")
        .foreach { sql =>
          logger.debug(sql)
          val st = conn.createStatement
          st.execute(sql)
          st.close
        }
      val st = conn.createStatement
      st.execute("create sequence seq")
      st.close
      logger.debug("Database created successfully.")

      override val tresqlResources  = new TresqlResources {
        override val resourcesTemplate =
          super.resourcesTemplate.copy(
            conn = ValidationSpecs.this.conn,
            metadata = querease.tresqlMetadata,
            dialect = dialects.HSQLDialect,
            idExpr = s => "nextval('seq')"
          )
      }

      override def dbUse[A](a: => A)(implicit timeout: QueryTimeout, pool: PoolName): A =
        try a finally tresqlResources.conn.rollback
      override protected def transactionInternal[A](forceNewConnection: Boolean, a: => A)(implicit timeout: QueryTimeout,
                                                                                          pool: PoolName): A =
        try a finally tresqlResources.conn.commit
    }

    testApp = new ValidationSpecsApp(db)
  }

  implicit val usr = TestUsr(1)
  implicit val state: Map[String, Any] = Map.empty
  implicit val timeout: QueryTimeout = DefaultQueryTimeout.get

  implicit val f02 = jsonFormat2(querease.ValidationResult)

  it should "validate on save" in {
    val dto = new validations_test

    dto.int_col = 3
    intercept[BusinessException] {
      testApp.saveApp(dto)
    }.getMessage.parseJson.convertTo[List[querease.ValidationResult]] should be(List(querease.ValidationResult(Nil,
      List("int_col should be greater than 5 but is 3", "int_col should be greater than 10 but is 3")
    )))

    dto.int_col = 7
    intercept[BusinessException] {
      testApp.saveApp(dto)
    }.getMessage.parseJson.convertTo[List[querease.ValidationResult]] should be(List(querease.ValidationResult(Nil,
      List("int_col should be greater than 10 but is 7")
    )))

    dto.int_col = 11
    testApp.saveApp(dto) should be(0)

    dto.int_col = 13
    intercept[BusinessException] {
      testApp.saveApp(dto)
    }.getMessage.parseJson.convertTo[List[querease.ValidationResult]] should be(List(querease.ValidationResult(Nil,
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
    testApp.saveApp(dto) should be(0)
  }
}
