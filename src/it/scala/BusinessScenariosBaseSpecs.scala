package org.wabase

import java.io.File
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpHeader, HttpMethods, HttpResponse, Multipart}
import akka.http.scaladsl.model.headers.`Content-Type`
import akka.http.scaladsl.model.headers.RawHeader
import com.typesafe.config.{Config, ConfigFactory}
import org.mojoz.querease.TresqlMetadata
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers
import org.tresql.{Query, ThreadLocalResources}
import org.wabase.AppMetadata.DbAccessKey
import spray.json._

import scala.collection.immutable.{Map, Seq}
import scala.concurrent.Await
import scala.language.reflectiveCalls
import scala.util.{Random, Try}
import org.wabase.client.{ClientException, WabaseHttpClient}

abstract class BusinessScenariosBaseSpecs(val scenarioPaths: String*) extends FlatSpec with Matchers with WabaseHttpClient with BeforeAndAfterAll with TemplateUtil {

  import jsonConverter.MapJsonFormat
  val db = new DbAccess with Loggable {
    override protected def tresqlMetadata: TresqlMetadata = null
  }
  import db._

  implicit val queryTimeout: QueryTimeout = QueryTimeout(10)
  implicit val Cp: PoolName = DEFAULT_CP
  implicit val extraDb: Seq[DbAccessKey] = Nil

  protected lazy val config = ConfigFactory.load()
  protected lazy val testOnlyScenariousPattern = {
    val defaultPattern = ".+"
    val pattern =
      Option("business-scenarios.test-only").filter(config.hasPath).map(config.getString).getOrElse(defaultPattern)
    if (pattern != defaultPattern)
      logger.warn(s"Business scenarios test-only pattern: $pattern")
    pattern.r
  }

  protected lazy val testOnlyFilesPattern = {
    val defaultPattern = "^.*\\.yaml$"
    val pattern =
      Option("business-scenarios.test-only-files").filter(config.hasPath).map(config.getString).getOrElse(defaultPattern)
    if (pattern != defaultPattern)
      logger.warn(s"Business scenarios test-only-files pattern: $pattern")
    pattern.r
  }

  protected lazy val isFullCompareByDefault: Boolean = true

  override def beforeAll() = {
    login()
    listenToWs(deferredActor)
  }

  def recursiveListDirectories(f: File): Array[File] = {
    val these = Option(f.listFiles) getOrElse Array[File]()
    these.filter(_.isDirectory) ++
      these.filter(_.isDirectory).flatMap(recursiveListDirectories)
  }

  def isTestCaseFile(file: File): Boolean =
    file.isFile &&
      testOnlyFilesPattern.pattern.matcher(file.getName).matches

  def shouldTestScenario(scenario: File): Boolean =
    scenario.listFiles.exists(isTestCaseFile) &&
      testOnlyScenariousPattern.pattern.matcher(scenario.getName).matches

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

  def assertResponseHeaders(response: HttpResponse, expectedHeaders: Seq[HttpHeader]) = {
    val received = (response.headers.toSet + `Content-Type`(response.entity.contentType)).map(_.toString)
    expectedHeaders foreach { expectedHeader =>
      if (!received.contains(expectedHeader.toString))
        sys.error(s"Response did not contain expected header $expectedHeader. Headers received: ${received.toSeq.sorted.mkString(", ")}")
    }
  }

  private val randomStringPattern = "randomString\\((\\d*)\\)".r
  def templateFunctions: Map[String, Any] => PartialFunction[String, Any] = context => {
    case (randomStringPattern(length)) => Random.alphanumeric.take(length.toInt).mkString
  }

  private val placeholderPattern = """.*\{\{(.+)\}\}""".r
  def applyContext(map: Map[String, Any], context: Map[String, Any]): (Map[String, Any], Map[String, Any]) = {
    var newValues = Map.empty[String, Any]
    def mapString(s: String) = {
      def deriveFromContext(keyOrFunctionName: String) = {
        try {
          context.getOrElse(keyOrFunctionName, templateFunctions(context)(keyOrFunctionName))
        } catch {
          case util.control.NonFatal(ex) =>
            throw new RuntimeException(
              s"Key '$keyOrFunctionName' is not found in context and templateFunctions failed. " +
              s"Keys in context: [${context.keys.toSeq.sorted.mkString(", ")}].", ex)
        }
      }
      def applyPlaceholder(currentValue: String, placeholderName: String, value: Any) = {
        // Mustache like 'Template', for now it's enough
        val placeholder = s"{{$placeholderName}}"
        if (currentValue == placeholder)
          value
        else if (currentValue.indexOf(placeholder) >= 0)
          currentValue.replace(placeholder, s"${transformToStringValues(value)}")
        else
          currentValue
      }
      val kcPattern = "<-\\W*(.*)\\W*->\\W*(.*)\\W*".r
      val ckPattern = "->\\W*(.*)\\W*<-\\W*(.*)\\W*".r
      val kPattern = "<-\\W*(.*)".r

      val patched =
      if (s != null && s.contains("<-")) {
        val (keyOrFunctionName, cKey) = s.trim match {
          case kcPattern(keyOrFunctionName, cKey) => (keyOrFunctionName.trim, cKey.trim)
          case ckPattern(cKey, keyOrFunctionName) => (keyOrFunctionName.trim, cKey.trim)
          case kPattern (keyOrFunctionName)       => (keyOrFunctionName.trim, null)
        }
        val value = deriveFromContext(keyOrFunctionName)
        if (cKey != null) newValues += cKey.trim -> value
        value
      } else context.foldLeft(s: Any) { case (currentResult, (key, value)) => currentResult match {
        case currentResult: String =>
          applyPlaceholder(currentResult, key, value)
        case _ => currentResult
      }}
      patched match {
       case patchedS: String => patchedS match {
        case placeholderPattern(placeholderName) =>
          // TODO for all
          if (templateFunctions(context).isDefinedAt(placeholderName))
            applyPlaceholder(patchedS, placeholderName, templateFunctions(context)(placeholderName))
          else patchedS
        case _ => patchedS
       }
       case _ => patched
      }
    }
    val result = map.map(e => (e._1, e._2 match {
      case l: List[_] => l.map {
        case null => null
        case m: Map[String, _]@unchecked =>
          val (v, c) = applyContext(m, context)
          newValues ++= v
          c
        case s: String => mapString(s)
        case b: Boolean => b
        case d: Double => d
        case i: Int => i
        case l: Long => l
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
    scenario: File, testCase: File, context: Map[String, Any], map: Map[String, Any],
    path: String, method: String, params: Map[String, Any], requestInfo: RequestInfo,
    expectedHeaders: Seq[HttpHeader], expectedResponse: Any, expectedError: String,
    tresqlRow: String, tresqlList: String, tresqlTransaction: String, options: Seq[String],
  ): Unit = logger.whenDebugEnabled {
    import requestInfo._
    logger.debug(Seq(
      "=========================",
      "scenario:           " + scenario.getName,
      "test case:          " + testCase,
      "path:               " + path,
      "method:             " + method,
      "params:             " + Option(params).filter(_.nonEmpty),
      "headers:            " + Option(headers).filter(_.nonEmpty)
                                 .map(_.map(_.toString).toSeq.sorted.mkString(", ")).getOrElse(""),
      "request json        " + Option(requestMap).map(_.toJson.compactPrint).getOrElse(""),
      "request string:     " + Option(requestString).getOrElse(""),
      "expected headers:   " + Option(expectedHeaders).filter(_.nonEmpty)
                                 .map(_.map(_.toString).toSeq.sorted.mkString(", ")).getOrElse(""),
      "expected response:  " + Option(expectedResponse).getOrElse(""),
      "expected error:     " + Option(expectedError).getOrElse(""),
      "response options:   " + Option(options).map(_.mkString(", ")).getOrElse(""),
      "tresql row:         " + Option(tresqlRow).getOrElse(""),
      "tresql list:        " + Option(tresqlList).getOrElse(""),
      "tresql transaction: " + Option(tresqlTransaction).getOrElse(""),
      "raw test case data: " + Option(map).getOrElse(""),
      "context:            " + Option(context).filter(_.nonEmpty)
                                 .map(_.map { case (k, v) => s"$k=$v" }.toSeq.sorted.mkString(", ")).getOrElse(""),
      "=========================",
    ) .filter(_.length > "raw test case data: ".length)
      .mkString("\n", "\n", ""))
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

  def logScenarioResponseInfoOnFailure(
    scenario: File, testCase: File, context: Map[String, Any], exception: Throwable,
    debugResponse: Boolean, rawResponse: Any, response: Any,
  ): Unit = {
    if (debugResponse) {
      val fullTestName = s"${scenario.getName}/${testCase.getName}"
      logger.info(s"\n**** Response causing $fullTestName to fail with '${exception.getMessage}':\n$rawResponse\n****")
    }
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

  def checkTestCase(scenario: File, testCase: File, context: Map[String, Any], map: Map[String, Any], retriesLeft: Int): Map[String, Any] = {
    val path = map.s("path")
    val method = map.sd("method", "GET")
    val params = map.m("params")
    val requestInfo = extractRequestInfo(cleanupTemplate(map), method)
    import requestInfo._
    val fullCompare   = map.bd("full_compare", isFullCompareByDefault)
    val mergeResponse = map.b("merge_response")
    val debugResponse = map.get("debug_response")
      .map { case false => false case _ => true }.getOrElse(true)
    val expectedError = map.sd("error", null)
    val expectedResponse = (map.getOrElse("response", null), requestMap) match{
      case (resp, _) if !mergeResponse => resp
      case (resp, null) => resp
      case (null, req) => req
      case (resp : Map[String, Any] @unchecked, req) => cleanupTemplate(mergeTemplate(req, resp))
    }
    val expectedHeaders = map.m("response_headers").map {
      case ("Content-Type", value) => // Content-Type is not accepted as valid RawHeader
        `Content-Type`.parseFromValueString(value.toString).toOption.get
      case (name, value) =>
        RawHeader(name, value.toString)
    }.toList
    val tresqlRow = map.sd("tresql_row", null)
    val tresqlList = map.sd("tresql_list", null)
    val tresqlTransaction = map.sd("tresql_transaction", null)
    val options = Seq(
      if (fullCompare)      "full compare" else "partial compare",
      if (mergeResponse)    "merge"        else "no merge",
      if (debugResponse)    "debug"        else "no debug",
      if (retriesLeft > 0) s"retries left: $retriesLeft" else "",
    ).filter(_ != "")
    logScenarioRequestInfo(
      scenario, testCase, context, map,
      path, method, params, requestInfo,
      expectedHeaders, expectedResponse, expectedError,
      tresqlRow, tresqlList, tresqlTransaction, options,
    )

    def doRequest: HttpResponse  = (method, requestMap, requestString, requestBytes, requestFormData) match {
      case ("GET",   null, null,   null, null) => httpGetAwait [HttpResponse](path, params, headers)
      case ("POST",   map, null,   null, null) => httpPostAwait[JsValue,     HttpResponse](HttpMethods.POST,   path, map.toJson, headers)
      case ("POST",  null, string, null, null) => httpPostAwait[String,      HttpResponse](HttpMethods.POST,   path, string,     headers)
      case ("POST",  null, null,  bytes, null) => httpPostAwait[Array[Byte], HttpResponse](HttpMethods.POST,   path, bytes,      headers)
      case ("POST",  null, null,   null, form) => httpPostAwait[
        Multipart.FormData, HttpResponse](HttpMethods.POST,   path, form,       headers)
      case ("PUT",    map, null,   null, null) => httpPostAwait[JsValue,     HttpResponse](HttpMethods.PUT,    path, map.toJson, headers)
      case ("PUT",   null, string, null, null) => httpPostAwait[String,      HttpResponse](HttpMethods.PUT,    path, string,     headers)
      case ("PUT",   null, null,  bytes, null) => httpPostAwait[Array[Byte], HttpResponse](HttpMethods.PUT,    path, bytes,      headers)
      case ("PUT",   null, null,   null, form) => httpPostAwait[
        Multipart.FormData, HttpResponse](HttpMethods.PUT,    path, form,       headers)
      case ("DELETE", null, null,  null, null) => httpPostAwait[String,      HttpResponse](HttpMethods.DELETE, path, "",         headers)
      case r => sys.error("Unsupported request type: "+r)
    }

    val unprocessedResponse =
      if (tresqlRow != null)
        transformToStringValues(dbUse(Query(tresqlRow, context).toListOfMaps.headOption.getOrElse(Map())))
      else if (tresqlList != null)
        transformToStringValues(dbUse(Query(tresqlList, context).toListOfMaps))
      else if (tresqlTransaction != null) {
        transaction(Query(tresqlTransaction, context))
        Map("result" -> "ok")
      } else if (expectedError == null) {
        doRequest
      } else {
        val message = intercept[ClientException](doRequest).getMessage
        message should include (expectedError)
        message
      }

    val (rawResponse, response) = unprocessedResponse match {
      case httpResponse: HttpResponse =>
        val resString = Await.result(httpResponse.entity.toStrict(awaitTimeout), awaitTimeout).data.utf8String
        Try(JsonToAny(resString.parseJson)).toOption.map((resString, _)).getOrElse((resString, resString))
      case _ => (unprocessedResponse, unprocessedResponse)
    }

    logScenarioResponseInfo(debugResponse, response)

    if (expectedHeaders.nonEmpty)
      unprocessedResponse match {
        case httpResponse: HttpResponse =>
          assertResponseHeaders(httpResponse, expectedHeaders)
        case x => sys.error(s"Unexpected response class for header tests: ${x.getClass.getName}")
      }

    if(expectedResponse != null) try assertResponse(response, expectedResponse, "[ROOT]", fullCompare) catch {
      case util.control.NonFatal(ex) =>
        logScenarioResponseInfoOnFailure(scenario, testCase, context, ex, debugResponse, rawResponse, response)
      throw ex
    }
    else Map.empty[String, String]
  }

  def checkTestCase(scenario: File, testCase: File, context: Map[String, Any], map: Map[String, Any]): Map[String, Any] = {
    val retries = map.get("retries").map(_.toString.toInt).getOrElse(0)
    var result: Map[String, Any] = Map.empty
    import scala.util.control.Breaks._
    breakable {
      for (retriesLeft <- (0 to retries).reverse) {
        map.get("sleep").map(_.toString).map { time =>
          logger.info(s"Sleeping $time")
          val duration = scala.concurrent.duration.Duration(time)
          Thread.sleep(duration.toMillis)
          logger.info(s"Awake!")
        }
        try {
          result = checkTestCase(scenario, testCase, context, map, retriesLeft)
          break()
        } catch {
          case util.control.NonFatal(ex) if retriesLeft > 0 =>
            logger.info(s"Retrying, retries left: $retriesLeft (because failed with ${ex.getMessage})")
        }
      }
    }
    result
  }

  def ckeckAllTestCases =
    scenarios.sortBy(_.getCanonicalPath).foreach{scenario =>
      behavior of scenario.getName
      var context = Map.empty[String, Any]
      it should "login" in login()
      scenario.listFiles.filter(isTestCaseFile).sortBy(_.getName).foreach{testCase =>
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
