package org.wabase

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import com.typesafe.sslconfig
import com.typesafe.sslconfig.ssl.{ConfigSSLContextBuilder, SSLConfigFactory}
import com.typesafe.sslconfig.ssl.{DefaultKeyManagerFactoryWrapper, DefaultTrustManagerFactoryWrapper}
import com.typesafe.sslconfig.util.NoDepsLogger

import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.pekko.http.scaladsl.{ConnectionContext, Http}
import org.apache.pekko.http.scaladsl.model.{HttpRequest, HttpResponse}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import scala.io.StdIn

class WabaseServer(wabase: WabaseService.Wabase) {
  val port = WabaseServer.port
  // start server event subscriber watcher actor
  wabase.system.actorOf(Props(classOf[ServerNotifications.EventSubscriberWatcher]),
    ServerNotifications.SubscriberWatcherActorName)
  private val deferredControl = new WabaseDeferredControl(wabase)(wabase.system)
  private val service         = new WabaseService

  def handle(req: HttpRequest): Future[HttpResponse] =
    service.handle(wabase, deferredControl)(req)(wabase.system)
}

object WabaseServer {

  class App(exec: Execution) extends WabaseApp[WabaseUser]
    with Execution
    with AppBase[WabaseUser]
    with NoAudit[WabaseUser]
    with DbAccess
    with NoCustomConstraintMessage
    with NoValidation
    with Marshalling
    with AppProvider[WabaseUser]
    with JsonConverterProvider
    {
      // Members declared in org.wabase.Execution
      override protected def execution: org.wabase.Execution = exec

      // Members declared in org.wabase.AppProvider
      override type App = AppBase[WabaseUser]
      override protected def initApp: App = this

      // Members declared in org.wabase.JsonConverterProvider
      override protected def initJsonConverter: org.wabase.JsonConverter[?] = qio
    }

  class SslConfigLogger(delegate: Logger) extends NoDepsLogger {
    def isDebugEnabled: Boolean = {
      var enabled = false
      delegate.whenDebugEnabled {
        enabled = true
      }
      enabled
    }
    def debug(msg: String): Unit = delegate.debug(msg)
    def info(msg:  String): Unit = delegate.info(msg)
    def warn(msg:  String): Unit = delegate.warn(msg)
    def error(msg: String): Unit = delegate.error(msg)
    def error(msg: String, throwable: Throwable): Unit = delegate.error(msg)
  }

  object SslConfigLoggerFactory extends sslconfig.util.LoggerFactory {
    def apply(name: String):    NoDepsLogger = new SslConfigLogger(Logger(name))
    def apply(clazz: Class[_]): NoDepsLogger = new SslConfigLogger(Logger(clazz))
  }

  val port = config.getInt("port")
  lazy val isSslEnabled =
    Option("app.server.ssl.enabled").filter(config.hasPath).map(config.getBoolean)
      .getOrElse(config.hasPath("app.server.ssl-config"))

  lazy val shutdownOnKeyPressEnter = config.getBoolean("app.server.shutdown-on-keypress-enter")
  lazy val shutdownOnBindFailed    = config.getBoolean("app.server.shutdown-on-bind-failed")

  lazy val (app, server, bindingFuture) = {
    implicit val serverSystem: ActorSystem  = ActorSystem("wabase-server")
    implicit val ec: ExecutionContext = serverSystem.dispatcher
    val executionImpl = new ExecutionImpl()(serverSystem)
    val app = new App(executionImpl)
    val server = new WabaseServer(app)
    val protocol        = if (isSslEnabled) "https" else "http"
    val hostPortString  = s"$protocol://localhost:${server.port}"
    val bindAddress     = config.getString("app.server.bind-address")
    val bindingFuture =
      if (isSslEnabled) {
        val sslConfigSettings = SSLConfigFactory.parse(
          Option("app.server.ssl-config").filter(config.hasPath).map(config.getConfig).getOrElse(ConfigFactory.empty)
            .withFallback(Option("ssl-config").filter(config.hasPath).map(config.getConfig).getOrElse(ConfigFactory.empty)))
        val sslContext =
          new ConfigSSLContextBuilder(
            SslConfigLoggerFactory,
            sslConfigSettings,
            new DefaultKeyManagerFactoryWrapper(sslConfigSettings.keyManagerConfig.algorithm),
            new DefaultTrustManagerFactoryWrapper(sslConfigSettings.trustManagerConfig.algorithm),
          ).build()
        val httpsServerConnectionContext =
          ConnectionContext.httpsServer(sslContext)
        Http().newServerAt(bindAddress, server.port).enableHttps(httpsServerConnectionContext).bind(server.handle)
      } else {
        Http().newServerAt(bindAddress, server.port).bind(server.handle)
      }
    bindingFuture.onComplete {
      case Success(_) =>
        println(
          "\n" +
          s"Server now online at $hostPortString" +
          (if (shutdownOnKeyPressEnter) "\nPress ENTER to stop..." else "") +
          "\n\n"
        )
      case Failure(ex) =>
        println(
          "\n" +
          s"FAILED to start server at $hostPortString because of: ${ex.getMessage}" +
          "\n\n"
        )
        app.system.terminate()
        if (shutdownOnBindFailed) {
          System.exit(1)
        }
    }
    (app, server, bindingFuture)
  }

  def apply(): WabaseServer = server

  def unbind(): Unit = {
    implicit val ec: ExecutionContext = app.executor
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete { _ =>   // and terminate actor system when done
        app.system.terminate()
      }
  }

  def shutdown(): Unit = {
    implicit val ec: ExecutionContext = app.executor
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete { _ =>   // and shutdown when done
        app.system.terminate()
        System.exit(0)
      }
  }

  def main(args: Array[String]): Unit = {
    apply()
    if (shutdownOnKeyPressEnter) {
      StdIn.readLine() // let it run until user presses enter
      shutdown()
    }
  }

  def hello(ctx: WabaseRequestContext): String = "Hello from wabase!"
}
