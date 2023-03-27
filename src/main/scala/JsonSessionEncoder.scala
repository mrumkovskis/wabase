package org.wabase

import spray.json.DefaultJsonProtocol._
import spray.json._

trait JsonSessionEncoder[User] { this: Authentication[User] with AppProvider[User] with Execution =>
  import app.qe._
  import app.qio._
  implicit def userFormat: JsonFormat[User]
  implicit def sessionFormat: RootJsonFormat[Authentication.Session[User]] = jsonFormat4(Authentication.Session[User])

  override def encodeSession(session: Authentication.Session[User]) = session.toJson.toString
  override def decodeSession(session: String) = session.parseJson.convertTo[Authentication.Session[User]]
  override def userInfo(implicit user: User): String = user.toJson.compactPrint

  def dtoJsonFormat[D <: Dto: Manifest] = new RootJsonFormat[D] {
    def read(json: JsValue): D = fill[D](json.asJsObject)
    def write(usr: D) = toMap(usr).toJson
  }
}
