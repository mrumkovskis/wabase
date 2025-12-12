package org.wabase

import java.io.{File, PrintWriter}
import org.apache.pekko.http.scaladsl.model.{ContentTypes, HttpEntity, HttpHeader, HttpMethod, HttpMethods, HttpResponse, MediaType, MediaTypes, Multipart, RequestEntity}
import org.apache.pekko.http.scaladsl.model.headers.`Content-Type`
import org.apache.pekko.http.scaladsl.model.headers.RawHeader
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.http.scaladsl.server.directives.ContentTypeResolver
import com.typesafe.config.ConfigFactory
import org.apache.pekko.http.scaladsl.marshalling.{Marshaller, ToEntityMarshaller}
import org.apache.pekko.util.ByteString
import org.mojoz.querease.TresqlMetadata
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers
import org.tresql.Query
import org.wabase.AppMetadata.DbAccessKey

import java.time.Instant
import scala.collection.immutable.{Map, Seq}
import scala.concurrent.Await
import scala.language.reflectiveCalls
import scala.util.{Random, Try}
import org.wabase.client.{ClientException, HttpClientConfig, WabaseHttpClient}
import org.wabase.ds.ConnectionPools.DEFAULT_CP
import org.wabase.ds.{PoolName, QueryTimeout}

abstract class BusinessScenariosBaseSpecs(val scenarioPaths: String*)
       extends FlatSpec with Matchers with BeforeAndAfterAll
          with TemplateUtil with QuereaseProvider with Loggable {

  val db = new DbAccess with QuereaseProvider with Loggable {
    override protected def tresqlMetadata: TresqlMetadata = null
    override protected def initQuerease: AppQuerease = null
    override protected def initQuereaseIo: AppQuereaseIo[Dto] = null
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

  override protected def initQuerease: AppQuerease           = DefaultAppQuerease
  def initHttpClient: WabaseHttpClient = new WabaseHttpClient(HttpClientConfig("test")) {
    override protected def initQuerease: AppQuerease           = qe
  }
  final lazy val httpClient = initHttpClient
  import httpClient._

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
    scenario <- {
      val scenariosDirectory = new File(resourcePath + scenarioPath)
      logger.info(s"Looking for scenarios in ${scenariosDirectory.getAbsolutePath}")
      recursiveListDirectories(scenariosDirectory)
    }
    if shouldTestScenario(scenario)
  } yield scenario

  def assertResponse(response: Any, expectedResponse: Any, path: String, fullCompare: Boolean): Map[String, Any] = {
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
      case (a, s: String) if s.trim.startsWith("->") => Map(s.trim.substring(2).trim -> a)
      case (a, b) if b != null && String.valueOf(a) == b.toString => Map.empty
      case (null, null) => Map.empty
      case (a, b) => err(s"Element $a should be equal to $b")
    }
  }

  def assertResponseStatus(response: HttpResponse, expectedStatus: String) = {
    if (response.status.toString != expectedStatus)
      sys.error(s"Unexpected response status: ${response.status.toString}. Expected: $expectedStatus.")
  }

  def assertResponseHeaders(response: HttpResponse, expectedHeaders: Seq[HttpHeader]) = {
    val received = (response.headers.toSet + s"Content-Type: ${response.entity.contentType}").map(_.toString)
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
        case m: Map[String, _]@unchecked =>
          val (v, c) = applyContext(m, context)
          newValues ++= v
          c
        case s: String => mapString(s)
        case x => x
      }
      case m: Map[String, _] @unchecked =>
        val (v, c) = applyContext(m, context)
        newValues ++= v
        c
      case s: String => mapString(s)
      case x => x
    }))
    (newValues, result)
  }

  protected def isMultipartFormData(mediaType: MediaType) =
    mediaType.mainType == MediaTypes.`multipart/form-data`.mainType &&
    mediaType.subType  == MediaTypes.`multipart/form-data`.subType

  case class RequestInfo(
    method:  String,
    path:    String,
    params:  Map[String, Any],
    headers: Seq[HttpHeader],
    requestBytes: Array[Byte],
    requestMap: Map[String, Any],
    requestSeq: Seq[Any],
    requestString: String,
    requestFormData: Multipart.FormData,
  )

  def extractRequestInfo(map: Map[String, Any]): RequestInfo = {
    extractRequestInfo(map, map.sd("method", "GET"), "request", "request-body-file", "request-parts")
  }
  def extractRequestInfo(map: Map[String, Any], method: String, bodyKey: String, fileKey: String, partsKey: String): RequestInfo = {
    val path    = map.sd("path", null)
    val params  = map.m("params")
    val headers = map.m("headers")
    val requestBytes = Try(map.s(fileKey)).toOption.map(readFileBytes).orNull
    val requestParts = Try(map.a(partsKey)).toOption.orNull
    val valueAsMap   = Try(map.md(bodyKey, null)).toOption.orNull
    val valueAsSeq   = Try(map(bodyKey).asInstanceOf[Seq[Any]]).toOption.orNull

    val parsedHeaders: Seq[HttpHeader] = Option(headers).getOrElse(Map.empty).map {
      case ("Content-Type", value) => // Content-Type is not accepted as valid RawHeader
        `Content-Type`.parseFromValueString(value.toString).toOption.get
      case (name, value) =>
        RawHeader(name, value.toString)
    }.toList

    val forcedContentTypeHeaderOpt = parsedHeaders.collectFirst { case cth: `Content-Type` => cth }
    val fileContentTypeOpt =
      if  (forcedContentTypeHeaderOpt.isEmpty && requestBytes != null)
           Some(ContentTypeResolver.Default(map.s(fileKey)))
            .flatMap(ct => `Content-Type`.parseFromValueString(ct.toString).toOption)
      else None

    val bodyParts =
      if (requestParts != null) {
        requestParts map { partMap =>
          val partInfo = extractRequestInfo(partMap, method, "value", "file", "parts")
          val fieldName = Try(partMap.s("name")).toOption.getOrElse("file")
          val fileName = Try(partMap.s("filename")).toOption
            .orElse(Try(partMap.s("file")).toOption.map(path => (new File(path)).getName))
            .orNull
          val bodyEntity = Option((partInfo.requestMap, partInfo.requestString, partInfo.requestBytes) match {
            case ( map, null,   null) => HttpEntity(ContentTypes.`application/json`,         ResultEncoder.encodeAnyToJsonString(map))
            case (null, string, null) => HttpEntity(ContentTypes.`text/plain(UTF-8)`,        string)
            case (null, null,  bytes) => HttpEntity(ContentTypes.`application/octet-stream`, bytes)
            case r => sys.error("Unsupported multipart request part type: " + r)
          }).map { bodyEntity =>
            partInfo.headers.find(_.isInstanceOf[`Content-Type`])
              .map(ct => bodyEntity.withContentType(ct.asInstanceOf[`Content-Type`].contentType)).getOrElse(bodyEntity)
          }.get
          val additionalDispositionParams =
            Map(
              "filename" -> fileName,
            ).filter(_._2 != null)
          Multipart.FormData.BodyPart(
            fieldName,
            bodyEntity,
            additionalDispositionParams,
            partInfo.headers.filterNot(_.isInstanceOf[`Content-Type`]))
        }
      } else if (valueAsMap != null && forcedContentTypeHeaderOpt.exists(cth => isMultipartFormData(cth.contentType.mediaType))) {
        valueAsMap.map { case (k, v) =>
          val bodyEntity = v match {
            case map: Map[String @unchecked, Any @unchecked] => HttpEntity(ContentTypes.`application/json`,  ResultEncoder.encodeAnyToJsonString(map))
            case seq: Seq[Any]                               => HttpEntity(ContentTypes.`application/json`,  ResultEncoder.encodeAnyToJsonString(seq))
            case x                                           => HttpEntity(ContentTypes.`text/plain(UTF-8)`, s"$x")
          }
          Multipart.FormData.BodyPart(k, bodyEntity, Map.empty, Nil)
        }.toSeq
      } else {
        null
      }
    val requestFormData = Option(bodyParts).map(Multipart.FormData(_: _*)).orNull
    val requestString = Try(map.s(bodyKey)).toOption.getOrElse {
      if (valueAsMap != null && forcedContentTypeHeaderOpt.exists(_.contentType.mediaType == MediaTypes.`application/x-www-form-urlencoded`)) {
        val valueAsMapOfStrings = valueAsMap.transform {
          case (_, map: Map[String @unchecked, Any @unchecked]) => ResultEncoder.encodeAnyToJsonString(map)
          case (_, seq: Seq[Any])                               => ResultEncoder.encodeAnyToJsonString(seq)
          case (_, v)                                           => s"$v"
        }.toMap
        Uri.Query(valueAsMapOfStrings).toString
      } else null
    }
    val requestMap =
      if (bodyParts == null && requestString == null) {
        val defaultRequestMap: Map[String, Any] =
          if (requestBytes != null || requestParts != null || method == "GET" || method == "DELETE") null else Map.empty
        Try(map.md(bodyKey, defaultValue = defaultRequestMap)).toOption.orNull
      } else null
    val requestSeq = valueAsSeq
    RequestInfo(
      method, path, params,
      parsedHeaders ++ fileContentTypeOpt.toSeq,
      requestBytes, requestMap, requestSeq, requestString, requestFormData)
  }

  def logScenarioRequestInfo(
    scenario: File, testCase: File, context: Map[String, Any], map: Map[String, Any],
    requestInfo: RequestInfo,
    expectedStatus: String, expectedHeaders: Seq[HttpHeader], expectedResponse: Any, expectedError: String,
    options: Seq[String],
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
      "request json        " + Option(requestMap).map(ResultEncoder.encodeAnyToJsonString(_)).getOrElse(""),
      "request string:     " + Option(requestString).getOrElse(""),
      "expected status:    " + Option(expectedStatus).getOrElse(""),
      "expected headers:   " + Option(expectedHeaders).filter(_.nonEmpty)
                                 .map(_.map(_.toString).toSeq.sorted.mkString(", ")).getOrElse(""),
      "expected response:  " + Option(expectedResponse).getOrElse(""),
      "expected error:     " + Option(expectedError).getOrElse(""),
      "response options:   " + Option(options).map(_.mkString(", ")).getOrElse(""),
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

  def siblingFile(original: File, suffix: String): File = {
    val parentDir   = original.getParent
    val newFileName = original.getName + suffix
    val newFile =
      if  (parentDir != null)
           new File(parentDir, newFileName)
      else new File(newFileName)
    newFile
  }
  def createSiblingTextFile(original: File, suffix: String, content: String): File = {
    val newFile = siblingFile(original, suffix)
    val writer = new PrintWriter(newFile, "UTF-8")
    try writer.write(content) finally writer.close()
    newFile
  }
  def deleteSiblingTextFile(original: File, suffix: String): Unit = {
    val newFile = siblingFile(original, suffix)
    try newFile.delete() catch { case util.control.NonFatal(ex) => }
  }
  def shouldDumpResponseToFile(scenario: File, testCase: File, rawResponse: Any) =
    s"$rawResponse".length > 1000
  def dumpResponseToFile(scenario: File, testCase: File, rawResponse: Any) =
    createSiblingTextFile(testCase, ".received", s"$rawResponse")
  def deleteResponseFile(scenario: File, testCase: File) =
    deleteSiblingTextFile(testCase, ".received")
  private def trimString(s: String, maxLength: Int): String = {
    if (s.length <= maxLength) s else s.substring(0, maxLength) + "..."
  }
  def logScenarioResponseInfoOnFailure(
    scenario: File, testCase: File, context: Map[String, Any], exception: Throwable,
    debugResponse: Boolean, rawResponse: Any, response: Any,
  ): Unit = {
    if (debugResponse) {
      val fullTestName = s"${scenario.getName}/${testCase.getName}"
      val trimmedMessage = trimString(exception.getMessage, 200)
      val dumpedToFile =
        if (shouldDumpResponseToFile(scenario, testCase, rawResponse)) {
          try {
            val targetFile = dumpResponseToFile(scenario, testCase, rawResponse)
            logger.info(s"\n**** Response causing $fullTestName to fail with '$trimmedMessage' dumped to file ${targetFile.getAbsolutePath}\n****")
            true
          } catch {
            case util.control.NonFatal(ex) =>
              logger.warn(s"\n**** Failed to dump response causing $fullTestName to fail to file: ${ex.getMessage}")
              false
          }
        } else false
      if (!dumpedToFile) {
        logger.info(s"\n**** Response causing $fullTestName to fail with '$trimmedMessage':\n$rawResponse\n****")
      }
    }
  }
  def scenarioTestCaseOnSuccess(
    scenario: File, testCase: File, context: Map[String, Any],
    debugResponse: Boolean, rawResponse: Any, response: Any,
  ): Unit = {
    if (debugResponse) {
      deleteResponseFile(scenario, testCase)
    }
  }

  def transformToStringValues(m: Any): Any = m match {
    case mm: Map[String@unchecked, _] => mm.map { case (key, value) => (key, transformToStringValues(value)) }
    case s: Seq[_] => s map transformToStringValues
    case t: java.time.temporal.Temporal => Format.convertToString(t)
    case t: java.util.Date              => Format.convertToString(t)
    case x => x
  }

  def isBackdoorPath(path: String) = path.startsWith("/backdoor/")
  def backdoorAction(requestInfo: RequestInfo, context: Map[String, Any], map: Map[String, Any]): Any = {
    import requestInfo._
    path match {
      case "/backdoor/current_time" =>
        Map("current_time" -> Instant.now().toString)
      case "/backdoor/tresql_row" =>
        transformToStringValues(dbUse(Query(requestString, context).toListOfMaps.headOption.getOrElse(Map())))
      case "/backdoor/tresql_list" =>
        transformToStringValues(dbUse(Query(requestString, context).toListOfMaps))
      case "/backdoor/tresql_transaction" =>
        transaction(Query(requestString, context))
        Map("result" -> "ok")
      case _ =>
        throw new IllegalArgumentException(s"Unexpected path: $path")
    }
  }

  private implicit val seqOfAnyMarshaller: ToEntityMarshaller[Seq[Any]] = Marshaller.combined { item =>
    HttpEntity(ContentTypes.`application/json`, ResultEncoder.encodeAnyToJsonByteString(item))
  }
  def checkTestCase(scenario: File, testCase: File, context: Map[String, Any], map: Map[String, Any], retriesLeft: Int): Map[String, Any] = {
    val requestInfo = extractRequestInfo(cleanupTemplate(map))
    import requestInfo._
    val fullCompare   = map.bd("full_compare", isFullCompareByDefault)
    val mergeResponse = map.b("merge_response")
    val debugResponse = map.get("debug_response").forall { case false => false case _ => true }
    val expectedError = map.sd("error", null)
    val expectedStatus= map.sd("response_status", null)
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
    val options = Seq(
      if (fullCompare)      "full compare" else "partial compare",
      if (mergeResponse)    "merge"        else "no merge",
      if (debugResponse)    "debug"        else "no debug",
      if (retriesLeft > 0) s"retries left: $retriesLeft" else "",
    ).filter(_ != "")
    logScenarioRequestInfo(
      scenario, testCase, context, map,
      requestInfo,
      expectedStatus, expectedHeaders, expectedResponse, expectedError,
      options,
    )

    def httpPostAwaitMultipartFormData(method: HttpMethod, path: String, formData: Multipart.FormData, headers: Seq[HttpHeader]) = {
      val boundaryOpt =
        headers.collectFirst { case cth: `Content-Type` => cth }
          .map(_.contentType.mediaType)
          .filter(isMultipartFormData)
          .flatMap(_.params.get("boundary"))
      val (entity, requestHeaders) = boundaryOpt match {
        case Some(boundary) =>
          (formData.toEntity(boundary), headers)
        case None =>
          (formData.toEntity, headers.filterNot(_.isInstanceOf[`Content-Type`]))
      }
      httpPostAwait[RequestEntity, HttpResponse](HttpMethods.PUT, path, entity, requestHeaders)
    }

    def doRequest: HttpResponse  = (method, requestMap, requestSeq, requestString, requestBytes, requestFormData) match {
      case ("GET",   null, null, null,   null, null) => httpGetAwait [HttpResponse](path, params, headers)
      case ("POST",   map, null, null,   null, null) => httpPostAwait[Map[String, Any],     HttpResponse](HttpMethods.POST,path, map, headers)
      case ("POST",  null,  seq, null,   null, null) => httpPostAwait[Seq[Any],    HttpResponse](HttpMethods.POST,   path, seq,       headers)
      case ("POST",  null, null, string, null, null) => httpPostAwait[String,      HttpResponse](HttpMethods.POST,   path, string,    headers)
      case ("POST",  null, null, null,  bytes, null) => httpPostAwait[Array[Byte], HttpResponse](HttpMethods.POST,   path, bytes,     headers)
      case ("POST",  null, null, null,   null, form) => httpPostAwaitMultipartFormData          (HttpMethods.POST,   path, form,      headers)
      case ("PUT",    map, null, null,   null, null) => httpPostAwait[Map[String, Any],     HttpResponse](HttpMethods.PUT, path, map, headers)
      case ("PUT",   null,  seq, null,   null, null) => httpPostAwait[Seq[Any],    HttpResponse](HttpMethods.PUT,    path, seq,       headers)
      case ("PUT",   null, null, string, null, null) => httpPostAwait[String,      HttpResponse](HttpMethods.PUT,    path, string,    headers)
      case ("PUT",   null, null, null,  bytes, null) => httpPostAwait[Array[Byte], HttpResponse](HttpMethods.PUT,    path, bytes,     headers)
      case ("PUT",   null, null, null,   null, form) => httpPostAwaitMultipartFormData          (HttpMethods.PUT,    path, form,      headers)
      case ("DELETE",null, null, null,   null, null) => httpPostAwait[String,      HttpResponse](HttpMethods.DELETE, path, "",        headers)
      case r => sys.error("Unsupported request type: "+r)
    }

    val unprocessedResponse =
      if (isBackdoorPath(path)) {
        backdoorAction(requestInfo, context, map)
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
        expectedResponse match {
          case _: String => (resString, resString)
          case _ =>
            Try(CborOrJsonAnyValueDecoder.decode(ByteString(resString)))
              .toOption.map((resString, _))
              .getOrElse((resString, resString))
        }
      case _ => (unprocessedResponse, unprocessedResponse)
    }

    logScenarioResponseInfo(debugResponse, response)

    if (expectedStatus != null)
      unprocessedResponse match {
        case httpResponse: HttpResponse =>
          assertResponseStatus(httpResponse, expectedStatus)
        case x => sys.error(s"Unexpected response class for status tests: ${x.getClass.getName}")
      }

    if (expectedHeaders.nonEmpty)
      unprocessedResponse match {
        case httpResponse: HttpResponse =>
          assertResponseHeaders(httpResponse, expectedHeaders)
        case x => sys.error(s"Unexpected response class for header tests: ${x.getClass.getName}")
      }

    if (expectedResponse != null)
     try {
      val result = assertResponse(response, expectedResponse, "[ROOT]", fullCompare)
      scenarioTestCaseOnSuccess(scenario, testCase, context, debugResponse, rawResponse, response)
      result
     } catch {
      case util.control.NonFatal(ex) =>
        logScenarioResponseInfoOnFailure(scenario, testCase, context, ex, debugResponse, rawResponse, response)
      throw ex
     }
    else Map.empty[String, Any]
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

  protected def scenariosAutoLogin  = true
  protected def scenariosAutoLogout = true
  def ckeckAllTestCases =
    scenarios.sortBy(_.getCanonicalPath).foreach{scenario =>
      behavior of scenario.getName
      var context = Map.empty[String, Any]
      if (scenariosAutoLogin) {
        it should "login" in login()
      }
      scenario.listFiles.filter(isTestCaseFile).sortBy(_.getName).foreach{testCase =>
        it should "handle "+testCase.getName in {
          val (newValuesInContext, map) = applyContext(readPojoMap(testCase, getTemplatePath), context)
          context ++= newValuesInContext
          context ++= checkTestCase(scenario, testCase, context, map)
        }
      }
      if (scenariosAutoLogout) {
        it should "logout" in clearCookies
      }
    }

  ckeckAllTestCases
}
