package org.wabase

import akka.actor.ActorSystem
import org.wabase.DeferredControl.DeferredStorage

case class TestUsr(id: Long) {
  def toMap: Map[String, Any] = Map("id" -> id)
}

object TestDbAccess extends DbAccess with Loggable {
  override val tresqlResources  = new TresqlResources {
    override val resourcesTemplate = super.resourcesTemplate.copy(metadata = DefaultAppQuerease.tresqlMetadata)
  }
}

trait TestApp extends AppBase[TestUsr] with NoAudit[TestUsr] with PostgreSqlConstraintMessage with
  DbAccessDelegate with NoAuthorization[TestUsr] with AppFileStreamer[TestUsr] with AppConfig with
  DefaultValidationEngine {
  override type QE = AppQuerease
  override protected def initQuerease: QE = DefaultAppQuerease
  override def dbAccessDelegate: DbAccess = TestDbAccess
  override val I18nResourceName = "test"
}

object TestApp extends TestApp

class TestAppService(system: ActorSystem) extends ExecutionImpl()(system)
    with AppServiceBase[TestUsr]
    with AppFileServiceBase[TestUsr]
    with AppConfig
    with AppVersion
    with DefaultAppExceptionHandler[TestUsr]
    with DefaultWsInitialEventsPublisher
    with WsNotifications
    with Authentication[TestUsr]
    with DeferredControl
    with NoServerStatistics
    with Loggable
    with CSRFDefence {
  override type App = TestApp
  override def initApp: App = TestApp
  override def initFileStreamer: TestApp = TestApp
  override def encodeSession(session: Authentication.Session[TestUsr]): String = ???
  override def decodeSession(session: String) = ???
  override def signInUser = ???
  override def appVersion: String = "TEST"
  override protected def initDeferredStorage: DeferredStorage = new DbDeferredStorage(appConfig, this, dbAccess, this)
}

object YamlUtils {
  def parseYamlData(yamlStr: String): Any = {
    import org.snakeyaml.engine.v2.api.LoadSettings
    import org.snakeyaml.engine.v2.api.Load
    import scala.jdk.CollectionConverters._
    def toScalaType(v: Any): Any = v match {
      case m: java.util.Map[_, _] => m.asScala.map { case (k, v) => (k, toScalaType(v)) }.toMap
      case a: java.util.ArrayList[_] => a.asScala.map(toScalaType).toList
      case x => x
    }
    def stripMargin(s: String) =
      Option(s).map(_.replaceAll("^\\n+", "").split("\\n")).filter(_.nonEmpty).map { parts =>
        (parts.head.indexWhere(_ != ' '), parts)
      }.map { case (idx, parts) => parts.map(s => s substring Math.min(idx, s.length)).mkString("\n") } getOrElse ""
    val loaderSettings = LoadSettings.builder()
      .setLabel("test data")
      .setAllowDuplicateKeys(false)
      .build()
    toScalaType(new Load(loaderSettings).loadFromString(stripMargin(yamlStr)))
  }
}
