package org.wabase

import java.io.File

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpHeader, HttpMethods, Multipart}
import akka.http.scaladsl.model.headers.`Content-Type`
import akka.http.scaladsl.model.headers.RawHeader
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers
import org.tresql.{Query, ThreadLocalResources}
import spray.json._

import scala.collection.immutable.Seq
import scala.language.reflectiveCalls
import scala.util.{Random, Try}
import org.wabase.client.{ClientException, CoreClient}

abstract class BusinessScenariosBaseSpecs(val scenarioPaths: String*) extends FlatSpec with Matchers with CoreClient with BeforeAndAfterAll with TemplateUtil {

  import jsonConverter.MapJsonFormat
  val db = new DbAccess with Loggable {
    override implicit val tresqlResources: ThreadLocalResources = new TresqlResources {}
  }
  import db._

  implicit val queryTimeout = QueryTimeout(10)

  override def beforeAll() = {
    login()
    listenToWs(deferredActor)
  }

  def recursiveListDirectories(f: File): Array[File] = {
    val these = Option(f.listFiles) getOrElse Array()
    these.filter(_.isDirectory) ++
      these.filter(_.isDirectory).flatMap(recursiveListDirectories)
  }

  val scenarios = for {
    scenarioPath <- scenarioPaths
    scenario <- recursiveListDirectories(new File(resourcePath + scenarioPath))
    if scenario.listFiles.exists(_.isFile)
  } yield scenario

  def assertResponse(response: Any, expectedResponse: Any, path: String): Map[String, String] = {
    def err(message: String) = sys.error(path + ": " + message)

    (response, expectedResponse) match {
      case (elements: Seq[_], Nil) if elements.nonEmpty => err("List must be empty")
      case (elements: Seq[_], Nil) if elements.isEmpty => Map.empty
      case (_, Nil) => err("Element should not be here") // TODO test this
      case (elements: Seq[_], list: List[_]) =>
        if (elements.size != list.size) err(s"List size ${elements.size} should be equal to ${list.size}")
        elements.zip(list).zipWithIndex.flatMap(e => assertResponse(e._1._1, e._1._2, path + "/" + e._2)).toMap
      case (elements: Map[String, Any]@unchecked, map: Map[String, Any]@unchecked) =>
        map.flatMap { case (key, expectedValue) =>
          elements.get(key) match {
            case None => err(s"Object should contain key: $key")
            case Some(value) => assertResponse(value, expectedValue, path + "/" + key)
          }
        }
      case (a, s: String) if s.trim.startsWith("->") => Map(s.trim.substring(2).trim -> String.valueOf(a))
      case (a, b) if b != null && String.valueOf(a) == b.toString => Map.empty
      case (null, null) => Map.empty
      case (a, b) => err(s"Element $a should be equal to $b")
    }
  }

  def templateFunctions: PartialFunction[(String, Map[String, String]), String] = {
    val randomStringPattern = "randomString\\((\\d*)\\)".r

    {
      case (randomStringPattern(length), context) => Random.alphanumeric.take(length.toInt).mkString
    }
  }

  def applyContext(map: Map[String, Any], context: Map[String, String]): (Map[String, String], Map[String, Any]) = {
    var newValues = Map.empty[String, String]
    def mapString(s: String) = {
      def patchString(key: String, cKey: String) = {
        val value = context.getOrElse(key, templateFunctions(key, context))
        if (cKey != null) newValues += cKey -> value
        value
      }
      val kcPattern = "<-\\W*(.*)\\W*->\\W*(.*)\\W*".r
      val ckPattern = "->\\W*(.*)\\W*<-\\W*(.*)\\W*".r
      val kPattern = "<-\\W*(.*)".r

      if (s != null && s.contains("<-")) s.trim match {
        case kcPattern(key, cKey) => patchString(key.trim, cKey.trim)
        case ckPattern(cKey, key) => patchString(key.trim, cKey.trim)
        case kPattern(key) => patchString(key.trim, null)
      } else context.foldLeft(s){case (string, (key, value)) => string.replace(s"{{$key}}", value)} // Mustache like 'Template', for now it's enough
    }
    val result = map.map(e => (e._1, e._2 match {
      case l: List[_] => l.map {
        case m: Map[String, _]@unchecked =>
          val (v, c) = applyContext(m, context)
          newValues ++= v
          c
        case s: String => mapString(s)
      }
      case m: Map[String, _] @unchecked =>
        val (v, c) = applyContext(m, context)
        newValues ++= v
        c
      case null => null
      case b: Boolean => b
      case d: Double => d
      case i: Int => i
      case l: Long => l
      case s => mapString(s.toString)
    }))
    (newValues, result)
  }

  case class RequestInfo(
                          headers: Seq[HttpHeader],
                          requestBytes: Array[Byte],
                          requestMap: Map[String, Any],
                          requestString: String,
                          requestFormData: Multipart.FormData,
                        )

  def extractRequestInfo(map: Map[String, Any], method: String): RequestInfo = {
    val headers = map.m("headers")
    val requestBytes = Try(map.s("request-body-file")).toOption.map(readFileBytes).orNull
    val requestParts = Try(map.a("request-parts")).toOption.orNull
    val bodyParts =
      if (requestParts != null) {
        requestParts map { partMap =>
          val partInfo = extractRequestInfo(partMap, method)
          val fieldName = Try(partMap.s("fieldname")).toOption.getOrElse("file")
          val fileName = Try(partMap.s("filename")).toOption
            .orElse(Try(map.s("request-body-file")).toOption.map(path => (new File(path)).getName))
            .getOrElse("file")
          val bodyEntity = Option((partInfo.requestMap, partInfo.requestString, partInfo.requestBytes) match {
            case ( map, null,   null) => HttpEntity(ContentTypes.`application/json`,         map.toJson.prettyPrint)
            case (null, string, null) => HttpEntity(ContentTypes.`text/plain(UTF-8)`,        string)
            case (null, null,  bytes) => HttpEntity(ContentTypes.`application/octet-stream`, bytes)
            case r => sys.error("Unsupported multipart request part type: " + r)
          }).map { bodyEntity =>
            partInfo.headers.find(_.isInstanceOf[`Content-Type`])
              .map(ct => bodyEntity.withContentType(ct.asInstanceOf[`Content-Type`].contentType)).getOrElse(bodyEntity)
          }.get
          Multipart.FormData.BodyPart(
            fieldName,
            bodyEntity,
            Map("filename" -> fileName),
            partInfo.headers.filterNot(_.isInstanceOf[`Content-Type`]))
        }
      } else {
        null
      }
    val requestFormData = Option(bodyParts).map(Multipart.FormData(_: _*)).orNull
    val defaultRequestMap: Map[String, Any] =
      if (requestBytes != null || requestParts != null || method == "GET" || method == "DELETE") null else Map.empty
    val requestMap = Try(map.m("request", defaultValue = defaultRequestMap)).toOption.orNull
    val requestString = Try(map.s("request")).toOption.orNull
    val parsedHeaders: Seq[HttpHeader] = headers.map {
      case ("Content-Type", value) => // Content-Type is not accepted as valid RawHeader
        `Content-Type`.parseFromValueString(value.toString).toOption.get
      case (name, value) =>
        RawHeader(name, value.toString)
    }.toList
    RequestInfo(parsedHeaders, requestBytes, requestMap, requestString, requestFormData)
  }

  def checkTestCase(scenario: File, testCase: File, context: Map[String, String], map: Map[String, Any]) = {
    val path = map.s("path")
    val method = map.s("method", "GET")
    val params = map.m("params")
    val requestInfo = extractRequestInfo(cleanupTemplate(map), method)
    import requestInfo._
    val mergeResponse = map.b("merge_response")
    val debugResponse = map.get("debug_response")
      .map { case false => false case _ => true }.getOrElse(true)
    val error = map.s("error", null)
    val expectedResponse = (map.getOrElse("response", null), requestMap) match{
      case (resp, _) if !mergeResponse => resp
      case (resp, null) => resp
      case (null, req) => req
      case (resp : Map[String, Any] @unchecked, req) => cleanupTemplate(mergeTemplate(req, resp))
    }
    val tresqlRow = map.s("tresql_row", null)
    val tresqlList = map.s("tresql_list", null)
    val tresqlTransaction = map.s("tresql_transaction", null)
    println("=========================")
    println("scenario: "+scenario.getName)
    println("testCase: "+testCase)
    println("rawTestCaseData: "+map)
    println("path: "+path)
    println("method: "+method)
    println("params: "+params)
    println("headers: "+headers)
    println("requestMap: "+requestMap)
    println("requestString: "+requestString)
    println("mergeResponse: "+mergeResponse)
    println("expectedResponse: "+expectedResponse)
    println("error: "+error)
    println("context: "+context)
    println("tresql_row: "+tresqlRow)
    println("tresql_list: "+tresqlList)
    println("tresql_transaction: "+tresqlTransaction)
    println("=========================")

    def doRequest = (method, requestMap, requestString, requestBytes, requestFormData) match {
      case ("GET",   null, null,   null, null) => httpGetAwait [String](path, params, headers)
      case ("POST",   map, null,   null, null) => httpPostAwait[JsValue,     String](HttpMethods.POST,   path, map.toJson, headers)
      case ("POST",  null, string, null, null) => httpPostAwait[String,      String](HttpMethods.POST,   path, string,     headers)
      case ("POST",  null, null,  bytes, null) => httpPostAwait[Array[Byte], String](HttpMethods.POST,   path, bytes,      headers)
      case ("POST",  null, null,   null, form) => httpPostAwait[
        Multipart.FormData, String](HttpMethods.POST,   path, form,       headers)
      case ("PUT",    map, null,   null, null) => httpPostAwait[JsValue,     String](HttpMethods.PUT,    path, map.toJson, headers)
      case ("PUT",   null, string, null, null) => httpPostAwait[String,      String](HttpMethods.PUT,    path, string,     headers)
      case ("PUT",   null, null,  bytes, null) => httpPostAwait[Array[Byte], String](HttpMethods.PUT,    path, bytes,      headers)
      case ("PUT",   null, null,   null, form) => httpPostAwait[
        Multipart.FormData, String](HttpMethods.PUT,    path, form,       headers)
      case ("DELETE", null, null,  null, null) => httpPostAwait[String,      String](HttpMethods.DELETE, path, "",         headers)
      case r => sys.error("Unsupported request type: "+r)
    }

    def tresqlTransformDate(m: Any):Any = m match {
      case mm: Map[String@unchecked, _] => mm.map { case (key, value) => (key, tresqlTransformDate(value))}
      case s: Seq[_] => s map tresqlTransformDate
      case t: java.sql.Timestamp => t.toString.substring(0, 19)
      case d: java.sql.Date => d.toString
      case d: java.util.Date => Format.humanDateTime(d)
      case x => x
    }

    val response =
      if (tresqlRow != null)
        tresqlTransformDate(dbUse(Query(tresqlRow, context).toListOfMaps.headOption.getOrElse(Map())))
      else if (tresqlList != null)
        tresqlTransformDate(dbUse(Query(tresqlList, context).toListOfMaps))
      else if (tresqlTransaction != null) {
        transaction(Query(tresqlTransaction, context))
        Map("result" -> "ok")
      } else if (error == null) {
        val res = doRequest
        Try(JsonToAny(res.parseJson)).toOption.getOrElse(res)
      } else {
        val message = intercept[ClientException](doRequest).getMessage
        message should include (error)
        message
      }

    println("=========================")
    if (debugResponse)
      println("response: "+response)
    else
      println("[some response]")
    println("=========================")
    if(expectedResponse != null) assertResponse(response, expectedResponse, "[ROOT]")
    else Map.empty[String, String]
  }

  def ckeckAllTestCases =
    scenarios.sortBy(_.getCanonicalPath).foreach{scenario =>
      behavior of scenario.getName
      var context = Map.empty[String, String]
      it should "login" in login()
      scenario.listFiles.sortBy(_.getName).foreach{testCase =>
        it should "handle "+testCase.getName in {
          val (newValuesInContext, map) = applyContext(readPojoMap(testCase, getTemplatePath), context)
          context ++= newValuesInContext
          context ++= checkTestCase(scenario, testCase, context, map)
        }
      }
      it should "logout" in clearCookies
    }

  ckeckAllTestCases
}
