package org.wabase

import akka.actor.ActorSystem
import org.wabase.DeferredControl.DeferredStorage

case class TestUsr(id: Long) {
  def toMap: Map[String, Any] = Map("id" -> id)
}

object TestDbAccess extends DbAccess with Loggable {
  override protected def tresqlMetadata = DefaultAppQuerease.tresqlMetadata
}

trait TestApp extends AppBase[TestUsr] with NoAudit[TestUsr] with PostgreSqlConstraintMessage
  with DbAccessDelegate with NoAuthorization[TestUsr] with AppFileStreamer[TestUsr] with AppConfig
  with DefaultValidationEngine {
  override protected def initQuerease = new TestQuerease("/no-metadata.yaml")
  override def dbAccessDelegate: DbAccess = TestDbAccess
  override val I18nResourceName = "test"
  override def useLegacyFlow(viewName: String, actionName: String): Boolean = viewName endsWith "_legacy_flow"
}

object TestApp extends TestApp

class TestAppService(system: ActorSystem) extends ExecutionImpl()(system)
    with AppServiceBase[TestUsr]
    with AppFileServiceBase[TestUsr]
    with AppConfig
    with AppVersion
    with DefaultAppExceptionHandler[TestUsr]
    with DefaultWsInitialEventsPublisher
    with ServerNotifications
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

class TestAppServiceNoDeferred(system: ActorSystem) extends ExecutionImpl()(system)
  with AppServiceBase[TestUsr]
  with AppFileServiceBase[TestUsr]
  with AppConfig
  with AppVersion
  with Authentication[TestUsr]
  with ConstantQueryTimeout
  with NoServerStatistics
  with Loggable
  with CSRFDefence {
  override type App = TestApp
  override def initApp: App = TestApp
  override def initFileStreamer: AppFileStreamer[TestUsr] = TestApp
  override def encodeSession(session: Authentication.Session[TestUsr]): String = ???
  override def decodeSession(session: String) = ???
  override def signInUser = ???
  override def appVersion: String = "TEST"
}

object YamlUtils {
  def parseYamlData(yamlStr: String): Any = {
    import org.snakeyaml.engine.v2.api.LoadSettings
    import org.snakeyaml.engine.v2.api.Load
    import scala.jdk.CollectionConverters._
    def toScalaType(k: String, v: Any): Any = v match {
      case m: java.util.Map[_, _] => m.asScala.map { case (k, v) => (k, toScalaType("" + k, v)) }.toMap
      case a: java.util.ArrayList[_] => a.asScala.map(toScalaType(null, _)).toList
      case d: String if k != null && k.endsWith("date") => java.sql.Date.valueOf(d)
      case t: String if k != null && k.endsWith("time") => java.sql.Timestamp.valueOf(t)
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
    toScalaType(null, new Load(loaderSettings).loadFromString(stripMargin(yamlStr)))
  }
}
