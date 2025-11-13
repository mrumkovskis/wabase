package org.wabase
package client

import com.typesafe.config.Config
import org.apache.pekko.http.scaladsl.model.{AttributeKey, HttpRequest, HttpResponse}
import org.wabase.AppQuerease.InjectionParametersContext

import scala.concurrent.Future

trait HttpClient {
  def doRequest(req: HttpRequest): Future[HttpResponse]
}

object HttpClient {
  sealed trait Mode
  case object ProxyMode extends Mode
  val ModeKey = AttributeKey[Mode]("http-client-mode")
}

object HttpClientConfig {
  val rootPath = "http-client"
  val httpTunablePaths =
    Set("server-port", "server-path", "server-ws", "request-timeout", "await-timeout", "ssl-config")
  lazy val componentConfs = ComponentConf.getConfigs(rootPath, httpTunablePaths)
  lazy val configs: Map[String, Config] = componentConfs.confs.toMap - "ssl-config"
  lazy val httpClientFactory: HttpClientFactory =
    getObjectOrNewInstance[HttpClientFactory](componentConfs.root, "factory-class", "http client factory")
  def apply(name: String): Config =
    configs.getOrElse(name, sys.error(s"Http client config for '$name' is not found, please configure $rootPath.$name"))
}

trait HttpClientFactory {
  def createHttpClients: Map[String, InjectionParametersContext => HttpRequest => Future[HttpResponse]]
}

object HttpClientFactory extends HttpClientFactory {
  def createHttpClients: Map[String, InjectionParametersContext => HttpRequest => Future[HttpResponse]] = {
    HttpClientConfig.configs.map { case (n, clientCfg) =>
      val client = clientCfg.getString("client-class") match {
        case "org.wabase.client.RestClient" =>
          new RestClient(clientCfg)
        case "org.wabase.client.WabaseHttpClient" =>
          new WabaseHttpClient(clientCfg)
        case other =>
          getObjectOrNewInstance[HttpClient](clientCfg, "client-class", "http client")
      }
      n -> ((_: InjectionParametersContext) => req => client.doRequest(req))
    }.toMap
  }
}
