package org.wabase
package client

import com.typesafe.config.Config
import org.apache.pekko.http.scaladsl.model.{HttpRequest, HttpResponse}
import org.wabase.AppQuerease.InjectionParametersContext

import scala.concurrent.Future

trait HttpClient {
  def doRequest(req: HttpRequest): Future[HttpResponse]
}

object HttpClientConfig {
  val rootPath = "http-client"
  lazy val configs: Map[String, Config] =
    ComponentConf.getConfigs(s"$rootPath.conf", rootPath)
      .toMap - "ssl-config"
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
