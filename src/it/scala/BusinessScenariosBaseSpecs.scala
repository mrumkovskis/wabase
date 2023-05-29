package org.wabase

import java.io.File
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpHeader, HttpMethods, Multipart}
import akka.http.scaladsl.model.headers.`Content-Type`
import akka.http.scaladsl.model.headers.RawHeader
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers
import org.tresql.{Query, ThreadLocalResources}
import org.wabase.AppMetadata.DbAccessKey
import spray.json._

import scala.collection.immutable.Seq
import scala.language.reflectiveCalls
import scala.util.{Random, Try}
import org.wabase.client.{ClientException, WabaseHttpClient}

abstract class BusinessScenariosBaseSpecs(val scenarioPaths: String*) extends FlatSpec with Matchers with WabaseHttpClient with BeforeAndAfterAll with TemplateUtil {

  import jsonConverter.MapJsonFormat
  val db = new DbAccess with Loggable {
    override implicit val tresqlResources: ThreadLocalResources = new TresqlResources {}
  }
  import db._

  implicit val queryTimeout: QueryTimeout = QueryTimeout(10)
  implicit val Cp: PoolName = DEFAULT_CP
  implicit val extraDb: Seq[DbAccessKey] = Nil

  override def beforeAll() = {
    login()
    listenToWs(deferredActor)
  }

  def recursiveListDirectories(f: File): Array[File] = {
    val these = Option(f.listFiles) getOrElse Array[File]()
    these.filter(_.isDirectory) ++
      these.filter(_.isDirectory).flatMap(recursiveListDirectories)
  }

  def shouldTestScenario(scenario: File): Boolean =
    scenario.listFiles.exists(_.isFile)

  val scenarios = for {
    scenarioPath <- scenarioPaths
    scenario <- recursiveListDirectories(new File(resourcePath + scenarioPath))
    if shouldTestScenario(scenario)
  } yield scenario

  def assertResponse(response: Any, expectedResponse: Any, path: String, fullCompare: Boolean): Map[String, String] = {
    def err(message: String) = sys.error(path + ": " + message)

    (response, expectedResponse) match {
      case (elements: Seq[_], Nil) if elements.nonEmpty => err("List must be empty")
      case (elements: Seq[_], Nil) if elements.isEmpty => Map.empty
      case (_, Nil) => err("Element should not be here") // TODO test this
      case (elements: Seq[_], list: List[_]) =>
        if (elements.size != list.size) err(s"List size ${elements.size} should be equal to ${list.size}")
        elements.zip(list).zipWithIndex.flatMap(e => assertResponse(e._1._1, e._1._2, path + "/" + e._2, fullCompare)).toMap
      case (responseMap: Map[String, Any]@unchecked, expectedMap: Map[String, Any]@unchecked) =>
        if (fullCompare)
          responseMap.keys.find(key => !expectedMap.contains(key)).foreach { unexpectedKey =>
            err(s"Object should not contain key: $unexpectedKey")
          }
        expectedMap.flatMap { case (key, expectedValue) =>
          responseMap.get(key) match {
            case None => err(s"Object should contain key: $key")
            case Some(value) => assertResponse(value, expectedValue, path + "/" + key, fullCompare)
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
    val requestMap = Try(map.md("request", defaultValue = defaultRequestMap)).toOption.orNull
    val requestString = Try(map.s("request")).toOption.orNull
    val parsedHeaders: Seq[HttpHeader] = headers.map {
      case ("Content-Type", value) => // Content-Type is not accepted as valid RawHeader
        `Content-Type`.parseFromValueString(value.toString).toOption.get
      case (name, value) =>
        RawHeader(name, value.toString)
    }.toList
    RequestInfo(parsedHeaders, requestBytes, requestMap, requestString, requestFormData)
  }

  def logScenarioRequestInfo(
    scenario: File, testCase: File, context: Map[String, String], map: Map[String, Any],
    path: String, method: String, params: Map[String, Any], requestInfo: RequestInfo,
    mergeResponse: Boolean, expectedResponse: Any, error: String,
    tresqlRow: String, tresqlList: String, tresqlTransaction: String,
  ): Unit = {
    import requestInfo._
    logger.debug(Seq(
      "=========================",
      "scenario:           " + scenario.getName,
      "testCase:           " + testCase,
      "rawTestCaseData:    " + map,
      "path:               " + path,
      "method:             " + method,
      "params:             " + params,
      "headers:            " + headers,
      "requestMap:         " + requestMap,
      "requestString:      " + requestString,
      "mergeResponse:      " + mergeResponse,
      "expectedResponse:   " + expectedResponse,
      "error:              " + error,
      "context:            " + context,
      "tresql_row:         " + tresqlRow,
      "tresql_list:        " + tresqlList,
      "tresql_transaction: " + tresqlTransaction,
      "=========================",
    ).mkString("\n", "\n", ""))
  }

  def logScenarioResponseInfo(debugResponse: Boolean, response: Any): Unit = {
    logger.debug(Seq(
      "=========================",
      if (debugResponse)
        "response:           " + response
      else
        "[some response]",
      "=========================",
    ).mkString("\n", "\n", ""))
  }

  def transformToStringValues(m: Any): Any = m match {
    case mm: Map[String@unchecked, _] => mm.map { case (key, value) => (key, transformToStringValues(value)) }
    case s: Seq[_] => s map transformToStringValues
    case t: java.sql.Timestamp => t.toString match {
      case s if s endsWith ".0" => s.substring(0, 19)
      case s => s
    }
    case d: java.sql.Date => d.toString
    case d: java.util.Date => Format.humanDateTime(d)
    case x => x
  }

  def checkTestCase(scenario: File, testCase: File, context: Map[String, String], map: Map[String, Any]) = {
    val path = map.s("path")
    val method = map.sd("method", "GET")
    val params = map.m("params")
    val requestInfo = extractRequestInfo(cleanupTemplate(map), method)
    import requestInfo._
    val fullCompare   = map.b("full_compare")
    val mergeResponse = map.b("merge_response")
    val debugResponse = map.get("debug_response")
      .map { case false => false case _ => true }.getOrElse(true)
    val error = map.sd("error", null)
    val expectedResponse = (map.getOrElse("response", null), requestMap) match{
      case (resp, _) if !mergeResponse => resp
      case (resp, null) => resp
      case (null, req) => req
      case (resp : Map[String, Any] @unchecked, req) => cleanupTemplate(mergeTemplate(req, resp))
    }
    val tresqlRow = map.sd("tresql_row", null)
    val tresqlList = map.sd("tresql_list", null)
    val tresqlTransaction = map.sd("tresql_transaction", null)
    logScenarioRequestInfo(
      scenario, testCase, context, map,
      path, method, params, requestInfo,
      mergeResponse, expectedResponse, error,
      tresqlRow, tresqlList, tresqlTransaction,
    )

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

    val response =
      if (tresqlRow != null)
        transformToStringValues(dbUse(Query(tresqlRow, context).toListOfMaps.headOption.getOrElse(Map())))
      else if (tresqlList != null)
        transformToStringValues(dbUse(Query(tresqlList, context).toListOfMaps))
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

    logScenarioResponseInfo(debugResponse, response)

    if(expectedResponse != null) assertResponse(response, expectedResponse, "[ROOT]", fullCompare)
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
