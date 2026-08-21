package org.wabase
package client

import com.typesafe.config.Config
import org.apache.pekko.actor.{Actor, ActorRef, ActorSystem, Props}
import org.apache.pekko.http.scaladsl.model._
import org.apache.pekko.http.scaladsl.model.headers.{BasicHttpCredentials, Host, HttpOrigin, Origin, RawHeader, Authorization => AuthorizationHeader}
import org.apache.pekko.http.scaladsl.model.ws.TextMessage
import org.apache.pekko.http.scaladsl.unmarshalling._
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.{ByteString, Timeout}

import scala.concurrent.Future
import scala.collection.immutable.{Seq => iSeq}
import DeferredControl.`X-Deferred-Hash`
import org.apache.pekko.http.scaladsl.marshalling.Marshaller
import org.apache.pekko.pattern.ask

import scala.concurrent.duration.FiniteDuration
import WabaseUnmarshallers._

class WabaseHttpClient(clientCfg: Config = HttpClientConfig.componentConfs.root)(implicit system: ActorSystem)
    extends RestClient(clientCfg)(system) with BasicJsonMarshalling with QuereaseProvider {

  import qe.classToViewNameMap
  import org.wabase.{Dto, DtoWithId}
  import WabaseHttpClient._

  private val originUri = Uri(config.getString("app.host"))
  private val originHeader = Origin(HttpOrigin(originUri.scheme, Host(originUri.authority.host, originUri.authority.port)))

  lazy val CSRFCookieName = "XSRF-TOKEN"
  lazy val CSRFHeaderName = "X-XSRF-TOKEN"

  def getDefaultApiHeaders(cookies: CookieMap) = {
    // Scope to [[serverPath]] (domain / path / Secure) — same jar filtering as outbound Cookie headers
    val cookie = cookies.getCookies(Uri(serverPath)).flatMap(_.cookies).find(_.name == CSRFCookieName)
    RawHeader("X-Requested-With", "XMLHttpRequest") :: originHeader :: cookie.map(c => List(RawHeader(CSRFHeaderName, c.value))).getOrElse(Nil)
  }

  private lazy val defaultUsername: String = clientCfg.getString("username")
  private lazy val defaultPassword: String = clientCfg.getString("password")

  def login(username: String = defaultUsername, password: String = defaultPassword) = {
    httpGetAwait[String]("api", headers = iSeq(AuthorizationHeader(BasicHttpCredentials(username, password))))
  }

  def save[T <: DtoWithId](dto: T): T = {
    val response = httpPostAwait[Map[String, Any], Map[String, Any]](if(dto.id == null) HttpMethods.POST else HttpMethods.PUT, pathForDto(dto.getClass, dto.id),
      dto.toMap)
    getDtoFromJson(dto.getClass, response)
  }

  def delete[T <: Dto](viewClass: Class[T], id: Long): Unit = httpPostAwait[String, Unit](HttpMethods.DELETE, pathForDto(viewClass, id), "")
  def get[T <: Dto](viewClass: Class[T], id: Long, params: Map[String, Any] = Map.empty): T = getDtoFromJson(viewClass, httpGetAwait[Map[String, Any]](pathForDto(viewClass, id), params))
  def list[T <: Dto](viewClass: Class[T], params: Map[String, Any]): List[T] =
    getDtoListFromJson(viewClass, httpGetAwait[iSeq[Map[String, Any]]](pathForDto(viewClass, null), params))
  def count[T <: Dto](viewClass: Class[T], params: Map[String, Any]): Int =
    httpGetAwait[String](pathForDtoCount(viewClass), params).toInt
  def listRaw[T <: Dto](viewClass: Class[T], params: Map[String, Any]): String = httpGetAwait[String](pathForDto(viewClass, null), params) /*in case response is not JSON*/

  override def httpGet[R](
    path: String,
    params: Map[String, Any],
    headers: iSeq[HttpHeader],
    cookieStorage: CookieMap = getCookieStorage,
    timeout: FiniteDuration,
    throwHttpErrors: Boolean = true,
    followRedirects: Boolean = true,
  )(implicit unmarshaller: FromResponseUnmarshaller[R]): Future[R] = {
    super.httpGet[(R, iSeq[HttpHeader])](
      path, params, headers ++ getDefaultApiHeaders(cookieStorage), cookieStorage, timeout,
      throwHttpErrors = throwHttpErrors, followRedirects = followRedirects,
    ).flatMap(handleDeferredResponse[R](cookieStorage, throwHttpErrors, followRedirects))
  }

  override def httpPost[T, R](
    method: HttpMethod,
    path: String,
    content: T,
    headers: iSeq[HttpHeader],
    cookieStorage: CookieMap = getCookieStorage,
    timeout: FiniteDuration,
    throwHttpErrors: Boolean = true,
    followRedirects: Boolean = true,
  )(implicit marshaller: Marshaller[T, MessageEntity], unmarshaller: FromResponseUnmarshaller[R]): Future[R] =
    super.httpPost(
      method, path, content, headers ++ getDefaultApiHeaders(cookieStorage), cookieStorage, timeout,
      throwHttpErrors = throwHttpErrors, followRedirects = followRedirects,
    )(marshaller = marshaller, unmarshaller = unmarshaller)

  def getDtoListFromJson[T <: Dto](viewClass: Class[T], elements: Seq[Map[String, Any]]): List[T] =
    elements.map(getDtoFromJson(viewClass, _)).toList

  def getDtoFromJson[T <: Dto](viewClass: Class[T], value: Map[String, Any]): T = viewClass.getConstructor().newInstance().fill(value)

  def pathForDto[T <: Dto](clzz: Class[T], id: jLong) = "data/" + urlEncoder(classToViewNameMap(clzz)) + Option(id).map("/" + _).getOrElse("")
  def pathForDtoCount[T <: Dto](clzz: Class[T]) = "count/"+urlEncoder(classToViewNameMap(clzz))


  val deferredActor = system.actorOf(Props(classOf[DeferredActor]))
  def deferredResultUri(hash: String) = s"deferred/$hash/result"

  def handleDeferredResponse[R](
    cookieStorage: CookieMap,
    throwHttpErrors: Boolean = true,
    followRedirects: Boolean = true,
  )(response: (R, iSeq[HttpHeader]))(implicit umarshaller: FromResponseUnmarshaller[R]) : Future[R] = {
    val (result, headers) = response
    extractDeferredHash(headers) match{
      case None => Future.successful(result)
      case Some(hash) =>
        implicit val askTimeout = Timeout(requestTimeout)
        cookieStorage.setCookiesFromHeaders(headers)
        (deferredActor ? GetDeferred(hash)).map{deferredResult => deferredResult.asInstanceOf[Map[String, Any]]("status") match {
          case DeferredControl.DEFERRED_OK => // OK
          case DeferredControl.DEFERRED_ERR => // ERR
          case _ => throw ClientException(s"Received error while processing deferred request: \n$deferredResult")
        }}.flatMap(_ => httpGet[R](deferredResultUri(hash), cookieStorage = cookieStorage,
          throwHttpErrors = throwHttpErrors, followRedirects = followRedirects))
    }
  }

  def extractDeferredHash(headers: iSeq[HttpHeader]): Option[String] =
    headers.flatMap{
      case `X-Deferred-Hash`(hash) => iSeq(hash)
      case _ => iSeq()
    }.headOption
}

object WabaseHttpClient{
  import RestClient.{WsClosed, WsFailed}
  case class GetDeferred(hash: String)

  class DeferredActor extends Actor with Loggable{
    val completeStatuses = Set(DeferredControl.DEFERRED_ERR, DeferredControl.DEFERRED_OK)
    override def receive = queueResults(Map.empty, Map.empty)

    def queueResults(receivedMessages: Map[String, Any], subscribers: Map[String, ActorRef]): Receive = {
      case TextMessage.Strict(text) => try{
        val newMap = CborOrJsonAnyValueDecoder.decode(ByteString(text)).asInstanceOf[Map[String, Any]]
          .filter { _._1 != "version" }
          .map {
            case (k, v) => (k, v.asInstanceOf[Map[String, Any]])
          }.filter(_._2("status") match {
            case status: String if completeStatuses(status) => true
            case _ => false
          })
        for {
          (newHash, newValue) <- newMap
          subscriber <- subscribers.get(newHash)
        } subscriber ! newValue
        context.become(queueResults(receivedMessages ++ newMap, subscribers))
      }catch{
        case e: Exception => logger.error(s"Error while parsing message: \n$text", e)
      }
      case GetDeferred(hash) =>
        if (receivedMessages.contains(hash))
          sender() ! receivedMessages(hash)
        else
          context.become(queueResults(receivedMessages, subscribers + (hash -> sender())))
      case WsFailed(ex) =>
        logger.warn("Websocket failed, shutting down DeferredActor", ex)
        context.stop(self)
      case WsClosed =>
        logger.info("Websocket closed, shutting down DeferredActor")
        context.stop(self)
    }
  }

  def fileUploadForm(entity: BodyPartEntity, fileName: String, fieldName: String = "file") =
    Multipart.FormData(
      Source.single(
        Multipart.FormData.BodyPart(
          fieldName,
          entity,
          if (fileName == null) Map() else Map("filename" -> fileName))))
}
