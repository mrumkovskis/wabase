package org.wabase

import java.sql.{Connection, DriverManager}

import org.mojoz.metadata.in.YamlMd
import org.mojoz.metadata.out.SqlGenerator
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers
import org.tresql.dialects

class QuereaseBase(metadataFile: String) extends AppQuerease {
  override type DTO = org.wabase.Dto
  override type DWI = org.wabase.DtoWithId
  override lazy val yamlMetadata = YamlMd.fromResource(metadataFile)
  override lazy val viewNameToClassMap = Map[String, Class[_ <: Dto]]()
}

class QuereaseBaseSpecs extends FlatSpec with Matchers with BeforeAndAfterAll with Loggable {

  protected var conn: Connection = _
  protected implicit var tresqlResources: TresqlResources = _
  protected var querease: AppQuerease = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    Class.forName("org.hsqldb.jdbc.JDBCDriver")
    this.conn = DriverManager.getConnection("jdbc:hsqldb:mem:querease_base_test")
    logger.debug(s"Creating database for ${getClass.getName} tests ...\n")
    SqlGenerator.hsqldb().schema(querease.tableMetadata.tableDefs)
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

    this.tresqlResources  = new TresqlResources {
      override val resourcesTemplate =
        super.resourcesTemplate.copy(
          conn = QuereaseBaseSpecs.this.conn,
          metadata = querease.tresqlMetadata,
          dialect = dialects.HSQLDialect,
          idExpr = s => "nextval('seq')"
        )
    }
  }
}
