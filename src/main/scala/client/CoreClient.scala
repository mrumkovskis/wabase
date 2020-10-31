package org.wabase
package client

import akka.actor.{Actor, ActorRef, Props}
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{BasicHttpCredentials, Host, HttpOrigin, Origin, RawHeader, Authorization => AuthorizationHeader}
import akka.http.scaladsl.model.ws.TextMessage
import akka.http.scaladsl.unmarshalling._
import akka.stream.scaladsl.Source
import akka.util.Timeout
import spray.json.{JsArray, JsObject, JsString, JsValue}

import scala.concurrent.Future
import scala.collection.immutable.{Seq => iSeq}
import spray.json._
import DeferredControl.`X-Deferred-Hash`
import akka.http.scaladsl.marshalling.Marshaller
import akka.pattern.ask

trait CoreClient extends RestClient with JsonConverterProvider with BasicJsonMarshalling {

  implicit lazy val qe: AppQuerease = initQuerease
  /** Override this method in subclass. Method usage instead of direct
  {{{val qe: AppQuerease}}} initialization ensures that this.qe and subclass qe
  have the same instance */
  protected def initQuerease: AppQuerease = DefaultAppQuerease

  import qe.classToViewNameMap
  import org.wabase.{Dto, DtoWithId}
  import CoreClient._
  import jsonConverter.MapJsonFormat

  private val originUri = Uri(config.getString("app.host"))
  private val originHeader = Origin(HttpOrigin(originUri.scheme, Host(originUri.authority.host, originUri.authority.port)))

  lazy val CSRFCookieName = "XSRF-TOKEN"
  lazy val CSRFHeaderName = "X-XSRF-TOKEN"

  def getDefaultApiHeaders(cookies: CookieMap) = {
    val cookie = cookies.getCookies.flatMap(_.cookies).find(_.name == CSRFCookieName)
    RawHeader("X-Requested-With", "XMLHttpRequest") :: originHeader :: cookie.map(c => List(RawHeader(CSRFHeaderName, c.value))).getOrElse(Nil)
  }

  def login(username: String = defaultUsername, password: String = defaultPassword) = {
    httpGet[String]("api", headers = iSeq(AuthorizationHeader(BasicHttpCredentials(username, password))))
  }

  // TODO use marshallers to convert from json to dto
  def save[T <: DtoWithId](dto: T): T = {
    val response = httpPost[JsValue, JsValue](if(dto.id == null) HttpMethods.POST else HttpMethods.PUT, pathForDto(dto.getClass, dto.id),
      dto.toMap.toJson)
    getDtoFromJson(dto.getClass, response)
  }

  def delete[T <: Dto](viewClass: Class[T], id: Long): Unit = httpPost[String, Unit](HttpMethods.DELETE, pathForDto(viewClass, id), "")
  def get[T <: Dto](viewClass: Class[T], id: Long, params: Map[String, Any] = Map.empty): T = getDtoFromJson(viewClass, httpGet[JsValue](pathForDto(viewClass, id), params))
  def list[T <: Dto](viewClass: Class[T], params: Map[String, Any]): List[T] =
    getDtoListFromJson(viewClass, httpGet[JsValue](pathForDto(viewClass, null), params))
  def count[T <: Dto](viewClass: Class[T], params: Map[String, Any]): Int =
    httpGet[String](pathForDtoCount(viewClass), params).toInt
  def listRaw[T <: Dto](viewClass: Class[T], params: Map[String, Any]): String = httpGet[String](pathForDto(viewClass, null), params) /*in case response is not JSON*/

  override def httpGetAsync[R](path: String, params: Map[String, Any], headers: iSeq[HttpHeader], cookieStorage: CookieMap = getCookieStorage)
                              (implicit unmarshaller: FromResponseUnmarshaller[R]): Future[R] = {
    super.httpGetAsync[(R, iSeq[HttpHeader])](path, params, headers ++ getDefaultApiHeaders(cookieStorage), cookieStorage = cookieStorage).flatMap(handleDeferredResponse[R](cookieStorage))
  }

  override def httpPostAsync[T, R](method: HttpMethod, path: String, content: T, headers: iSeq[HttpHeader], cookieStorage: CookieMap = getCookieStorage)
                                  (implicit marshaller: Marshaller[T, MessageEntity], unmarshaller: FromResponseUnmarshaller[R]): Future[R] =
    super.httpPostAsync(method, path, content, headers ++ getDefaultApiHeaders(cookieStorage), cookieStorage = cookieStorage)(marshaller = marshaller, unmarshaller = unmarshaller)

  def getDtoListFromJson[T <: Dto](viewClass: Class[T], jsValue: JsValue): List[T] = jsValue match{
    case JsArray(elements) => elements.map(getDtoFromJson(viewClass, _)).toList
    case _ => sys.error("Invalid response "+jsValue)
  }

  def getDtoFromJson[T <: Dto](viewClass: Class[T], value: JsValue): T = viewClass.getConstructor().newInstance().fill(value.asJsObject)

  def pathForDto[T <: Dto](clzz: Class[T], id: jLong) = "data/" + urlEncoder(classToViewNameMap(clzz)) + Option(id).map("/" + _).getOrElse("")
  def pathForDtoCount[T <: Dto](clzz: Class[T]) = "count/"+urlEncoder(classToViewNameMap(clzz))


  val deferredActor = system.actorOf(Props(classOf[DeferredActor]))
  def deferredResultUri(hash: String) = s"deferred/$hash/result"

  def handleDeferredResponse[R](cookieStorage: CookieMap)(response: (R, iSeq[HttpHeader]))(implicit umarshaller: FromResponseUnmarshaller[R]) : Future[R] = {
    val (result, headers) = response
    extractDeferredHash(headers) match{
      case None => Future.successful(result)
      case Some(hash) =>
        implicit val askTimeout = Timeout(requestTimeout)
        cookieStorage.setCookiesFromHeaders(headers)
        (deferredActor ? GetDeferred(hash)).map{deferredResult => deferredResult.asInstanceOf[JsObject].fields("status") match{
          case JsString(DeferredControl.DEFERRED_OK) => // OK
          case JsString(DeferredControl.DEFERRED_ERR) => // ERR
          case _ => throw ClientException(s"Received error while processing deferred request: \n$deferredResult")
        }}.flatMap(_ => httpGetAsync[R](deferredResultUri(hash), cookieStorage = cookieStorage))
    }
  }

  def extractDeferredHash(headers: iSeq[HttpHeader]): Option[String] =
    headers.flatMap{
      case `X-Deferred-Hash`(hash) => iSeq(hash)
      case _ => iSeq()
    }.headOption

  override protected def initJsonConverter: JsonConverter = qe
}

object CoreClient{
  import RestClient._
  case class GetDeferred(hash: String)

  class DeferredActor extends Actor with Loggable{
    val completeStatuses = Set(DeferredControl.DEFERRED_ERR, DeferredControl.DEFERRED_OK)
    override def receive = queueResults(Map.empty, Map.empty)

    def queueResults(receivedMessages: Map[String, JsObject], subscribers: Map[String, ActorRef]): Receive = {
      case TextMessage.Strict(text) => try{
        val newMap = text.parseJson.asJsObject.fields.map { case (k, v) => (k, v.asJsObject) }.filter(_._2.fields("status") match{
          case JsString(status) if completeStatuses(status) => true
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
      case WsClosed =>
        println("******************************** ACTOR SYSTEM SHUTDOWN ********************************")
        context.stop(self)
    }
  }

  def fileUploadForm(entity: BodyPartEntity, fileName: String, fieldName: String = "file") =
    Multipart.FormData(
      Source.single(
        Multipart.FormData.BodyPart(
          fieldName,
          entity,
          Map("filename" -> fileName))))

}
