package org.wabase

import akka.http.scaladsl.server.PathMatchers.{Remaining, Segment}
import akka.http.scaladsl.server.{Directive, Route, RoutingLog}
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, MediaType, StatusCodes}
import akka.http.scaladsl.model.headers.{ModeledCustomHeader, ModeledCustomHeaderCompanion}
import akka.http.scaladsl.server.Directives._
import akka.stream._
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.scaladsl.{Flow, GraphDSL, Sink, Source}
import akka.actor.{Actor, Props}

import scala.util.{Either, Left, Right, Success, Try}
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.concurrent.duration.{Duration, FiniteDuration}
import spray.json._
import DefaultJsonProtocol._
import DeferredControl.DeferredCheck
import DeferredControl.DeferredStatusPublisher

import scala.jdk.CollectionConverters._
import AppServiceBase._
import Authentication.SessionInfoRemover
import com.typesafe.config.Config
import AppFileStreamer.FileInfo

trait DeferredControl
  extends DeferredCheck with QueryTimeoutExtractor with DeferredStatusPublisher {
  this: Execution
   with JsonConverterProvider
   with AppExceptionHandler
   with AppConfig
   with AppStateExtractor
   with SessionInfoRemover
   with Marshalling
   with Loggable =>

  import DeferredControl._
  import HttpMessageSerialization._
  import jsonConverter.MapJsonFormat

  //this is not placed in DeferredControl object so that each instance of DeferredControl trait
  //subscribes to it's own notification message
  case object DeferredRequestArrived extends WsNotifications.Addressee

  lazy val defaultTimeout = DeferredControl.defaultTimeout
  lazy val deferredWorkerCount = DeferredControl.deferredWorkerCount
  lazy val deferredUris = DeferredControl.deferredUris
  lazy val deferredTimeouts = DeferredControl.deferredTimeouts
  lazy val deferredCleanupInterval = DeferredControl.deferredCleanupInterval

  protected val cleanupActor = system.actorOf(Props(classOf[DeferredControl.DeferredCleanup], this))

  protected def initDeferredStorage: DeferredStorage

  private val deferredStorage = Option(initDeferredStorage)
    .getOrElse(sys.error("initDeferredStorage function returned null, cannot initialize DeferredControl."))

  class DeferredQueue extends GraphStage[FanOutShape2[
      DeferredContext, DeferredContext, DeferredContext]] {
    val in = Inlet[DeferredContext]("in")
    val exe = Outlet[DeferredContext]("exe")
    val overflow = Outlet[DeferredContext]("overflow")
    val shape = new FanOutShape2(in, exe, overflow)
    val MaxQueueSize = 1024
    val QueueOverflowResponse = HttpResponse(StatusCodes.InternalServerError,
      entity = "Server too busy. Please try later again.")

    override def createLogic(attributes: Attributes) = new GraphStageLogic(shape) with OutHandler {
      var queue: scala.collection.mutable.Queue[DeferredContext] = _
      override def preStart() = {
        queue = scala.collection.mutable.Queue.empty
        pull(in)
      }
      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val ctx = grab(in)
          if (queue.size >= MaxQueueSize) {
            emit(overflow, ctx.copy(status = DEFERRED_ERR, result = QueueOverflowResponse))
          } else if (isAvailable(exe)) {
            val nctx = ctx.copy(status = DEFERRED_EXE)
            push(exe, nctx)
          } else {
            val nctx = ctx.copy(status = DEFERRED_QUEUE)
            queue.enqueue(nctx)
            deferredStorage.registerDeferredStatus(nctx)
            publishDeferredStatus(nctx)
          }
          pull(in)
        }
      })
      setHandler(exe, new OutHandler {
        override def onPull() = pushIfQueued
      })
      private def pushIfQueued = {
        if (queue.nonEmpty) {
          val nctx = queue.dequeue().copy(status = DEFERRED_EXE)
          push(exe, nctx)
        }
      }
      //does nothing
      override def onPull(): Unit = {}
      setHandler(overflow, this)
    }
  }

  val deferredGraph = GraphDSL.create(new DeferredQueue) { implicit b => deferredQueue =>
    import GraphDSL.Implicits._
    val entry = b.add(Flow
      .fromFunction(deferredStorage.registerDeferredRequest)
      .mapConcat(ctx => ctx.status match {
        case DeferredExists =>
          publishDeferredStatus(ctx)
          Nil
        case _ => List(ctx)
      }))
    entry.out ~> deferredQueue.in
    deferredQueue.out0 ~> Flow[DeferredContext] //exe port
      .map(deferredStorage.registerDeferredStatus)
      .map{ x => publishDeferredStatus(x); x}
      .mapAsyncUnordered(deferredWorkerCount)(executeDeferred)
      .mapAsyncUnordered(deferredWorkerCount)(deferredStorage.registerDeferredResult)
      .to(Sink.foreach(publishDeferredStatus))
    deferredQueue.out1 ~> Flow[DeferredContext] //overflow port
      .mapAsyncUnordered(deferredWorkerCount)(deferredStorage.registerDeferredResult)
      .to(Sink.foreach(publishDeferredStatus))
    SinkShape(entry.in)
  }

  //Start deferred request processing flow - subscribe entry actor to DeferredRequestArrived message
  Source.actorRef[DeferredContext](PartialFunction.empty, PartialFunction.empty, 8, OverflowStrategy.dropNew)
   .to(deferredGraph)
   .mapMaterializedValue(EventBus.subscribe(_, DeferredRequestArrived))
   .withAttributes(ActorAttributes.supervisionStrategy{
     case ex: Exception =>
       logger.error("DeferredGraph crashed", ex)
       Supervision.Resume
   }).run()

  /* ***********************
  **** Deferred phases *****
  **************************/
  def executeDeferred(ctx: DeferredContext) = {
    val processor = ctx.processor
    if (processor == null) sys.error(s"Cannot get processor for request: $ctx")
    Future { //launch processor in future since it is unknown what type of future it returns and whether it blocks
      processor(ctx.request).map(response => ctx.copy(
        result = response,
        status = if (response.status.intValue < 400) DEFERRED_OK else DEFERRED_ERR,
        responseTime = new Timestamp(currentTime)
      ))
    } flatMap identity
  }
  def publishDeferredStatus(ctx: DeferredContext) = {
    import EventBus._
    publish(Message(WsNotifications.UserAddressee(ctx.userIdString), ctx))
  }
  def publishUserDeferredStatuses(user: String): Unit = {
    val deferredRequests = deferredStorage.getUserDeferredStatuses(user)
    import EventBus._
    deferredRequests.foreach { ctx =>
      if (ctx.userIdString == user) publish(Message(WsNotifications.UserAddressee(user), ctx))
    }
  }
  /* end of deferred phases */

  /* ***********************
  ******* Directives *******
  **************************/
  def deferredRequestPath = path("deferred" / Segment / "request") & get
  def deferredResultPath = path("deferred" / Segment / "result") & get
  def isDeferredPath = pathPrefixTest(Segment ~ Remaining)
    .tfilter { case (segment, _) => deferredUris contains segment }
    .tflatMap { case (segment, _) =>
      mapRequest(_.addHeader(new `X-Deferred`(Left(true))))
    }

  def hasDeferredHeader = headerValuePF { case `X-Deferred`(timeoutString)
     if `X-Deferred`(timeoutString).timeout != Left(false) => }
    .flatMap(_ => pass)
  override def isDeferred = hasDeferredHeader

  def enableDeferred(user: String) = (isDeferredPath | hasDeferredHeader)
    .tflatMap(_ => deferred(user)) | pass


  def deferredTimeout(viewName: Option[String], timeout: Option[Int]): QueryTimeout = {
    val limit = viewName.flatMap(deferredTimeouts.get).getOrElse(defaultTimeout).toSeconds.toInt
    if (timeout.isDefined)
      timeout.filter(_ <= limit).map(QueryTimeout).getOrElse {
        throw new BusinessException(s"Max request timeout exceeded: ${timeout.get} > $limit")
      }
    else QueryTimeout(limit)
  }

  override
  def extractTimeout = headerValuePF { case `X-Deferred`(timeoutString) =>
      `X-Deferred`(timeoutString).timeout }
    .flatMap {
      //timeout specified in header value
      case Right(timeoutDuration) => provide(deferredTimeout(None, Some(timeoutDuration.toSeconds.toInt)))
      case Left(true) => pathPrefixTest(Segment ~ Remaining)
        .tflatMap { case (segment, _) =>
          provide(deferredTimeout(Some(segment), None))
        }
        .recover(_ => provide(deferredTimeout(None, None)))
      //provide default jdbc timeout X-Deferred negated
      case Left(false) => provide(queryTimeout)
    }
    .recover(_ => provide(queryTimeout)) //no deferred header provided - provide default jdbc timeout

  def deferred(user: String) = hasDeferredHeader.recover(_ =>
    mapRequest(_.addHeader(new `X-Deferred`(Right(defaultTimeout)))))
    .tflatMap(_ => extractRequestContext.flatMap { ctx => mapInnerRoute { route =>
      val wrappedRoute = handleExceptions(appExceptionHandler)(route)
      val requestProcessor =
        Route.toFunction { requestContext =>
          wrappedRoute(requestContext.withUnmatchedPath(ctx.unmatchedPath))
        }
      import EventBus._
      val hash = requestHash(user, ctx.request)
      val deferredCtx = DeferredContext(user, hash, ctx.request, requestProcessor)
      publish(Message(DeferredRequestArrived, deferredCtx))
      respondWithHeader(`X-Deferred-Hash`(hash))(complete(Map("deferred" -> hash).toJson))
    }
  })
  /* End of directives */

  /* ***********************
  ********* Routes *********
  *************************/
  def deferredRequest(hash: String, user: String) = {
    deferredStorage.getDeferredRequest(hash, user)
      .map(ctx => complete(Map(
          "url" -> ctx.request.uri.path.toString,
          "status" -> ctx.status
        ).toJson))
      .getOrElse(complete(StatusCodes.NotFound))
  }
  def deferredResultAction(hash: String, user: String) = {
    deferredStorage.getDeferredResult(hash, user)
      .map(complete(_))
      .getOrElse(complete(StatusCodes.NotFound))
  }
  def deferredHttpRequestAction(hash: String, user: String) = {
    deferredStorage.getDeferredHttpRequest(hash, user)
      .map(req => complete(
        Map[String, Any](
          "url" -> req.uri.path.toString,
          "state" -> extractState(req, ApplicationStateCookiePrefix)
        ).toJson))
      .getOrElse(complete(StatusCodes.NotFound))
  }
  //end of routes

  protected def requestHash(username: String, req: HttpRequest) = {
    val md = java.security.MessageDigest.getInstance("SHA-1")
    implicit val usr = username
    org.apache.commons.codec.binary.Base64.encodeBase64URLSafeString(
      md.digest(serialize((username, serializeHttpRequest(removeSessionInfoFromRequest(req))))))
  }

  protected def doCleanup: Int = deferredStorage.cleanupDeferredRequests

  def onRestartDeferred(): Unit = deferredStorage.onRestart()
}

object DeferredControl extends Loggable with AppConfig {
  trait DeferredCheck {
     def isDeferred: Directive[Unit]
  }
  trait NoDeferred extends DeferredCheck {
    override def isDeferred: Directive[Unit] = reject()
  }
  trait DeferredStatusPublisher {
    /** Publish user deferred request status info to user websocket */
    def publishUserDeferredStatuses(userIdString: String): Unit
  }

  val DEFERRED_OK    = "OK"
  val DEFERRED_ERR   = "ERR"
  val DEFERRED_QUEUE = "QUEUE"
  val DEFERRED_EXE   = "EXE"
  val DEFERRED_DEL   = "DEL"
  val DeferredExists = "EXISTS"

  lazy val defaultTimeout = Duration(Try(appConfig.getString("deferred-requests.default-timeout")).toOption.getOrElse("180s"))
  lazy val deferredWorkerCount = Try(appConfig.getInt("deferred-requests.worker-count")).toOption.getOrElse(2)
  lazy val deferredUris = Try(appConfig.getString("deferred-requests.requests").split("[\\s,]+").filter(_ != "").toSet).toOption.getOrElse(Set())
  lazy val deferredTimeouts = deferredUris.map(_ -> defaultTimeout).toMap ++
    Try(appConfig.getConfig("deferred-requests.timeouts")
      .entrySet.asScala.map(e => e.getKey -> Duration(e.getValue.unwrapped.toString)).toMap
    ).toOption.getOrElse(Map())
  lazy val deferredCleanupInterval = Duration(Try(appConfig.getString("deferred-requests.cleanup-job-interval")).toOption.getOrElse("1800s"))

  logger.info(s"defaultTimeout: $defaultTimeout")
  logger.info(s"deferredWorkerCount: $deferredWorkerCount")
  logger.info(s"deferredUris: $deferredUris")
  logger.info(s"deferredTimeouts: $deferredTimeouts")
  logger.info(s"deferredCleanupInterval: $deferredCleanupInterval")

  case class DeferredContext(
    userIdString: String,
    hash: String,
    request: HttpRequest,
    processor: (HttpRequest) => Future[HttpResponse],
    requestTime: Timestamp = new Timestamp(currentTime),
    result: HttpResponse = null,
    responseTime: Timestamp = null,
    status: String = DEFERRED_QUEUE,
    priority: Int = 0)

  case object RunDeferredCleanup
  case object GetProcessedDeferredCount
  case class ProcessedDeferredCount(count: Long)

  import logger._

  class DeferredCleanup(defControl: DeferredControl) extends Actor {
    var processedCount = 0L
    override def preStart() = {
      val fd = FiniteDuration(
        deferredCleanupInterval.length,
        deferredCleanupInterval.unit)
      context.system.scheduler.scheduleAtFixedRate(fd, fd, self, RunDeferredCleanup)(
        context.system.dispatcher)
      info(s"Deferred request cleanup job started with frequency $fd")
    }

    override def receive = {
      case RunDeferredCleanup =>
        //set status to DEL for timeouted requests
        info("DeferredCleanup started")
        processedCount += defControl.doCleanup
        info(s"DeferredCleanup job ended, total processed deferred count: $processedCount")
      case GetProcessedDeferredCount => sender() ! ProcessedDeferredCount(processedCount)
    }

    override def postStop() = {
      info("DeferredCleanup job stopped")
    }
  }
  object `X-Deferred` extends ModeledCustomHeaderCompanion[`X-Deferred`] {
    override val name = "X-Deferred"
    override def parse(value: String) = Try(new `X-Deferred`(Right(Duration(value))))
      .orElse(Try(new `X-Deferred`(Left(value.toBoolean))))
  }
  final class `X-Deferred`(val timeout: Either[Boolean, Duration]) extends ModeledCustomHeader[`X-Deferred`] {
    def renderInRequests = true
    def renderInResponses = false
    override val companion = `X-Deferred`
    override def value: String = timeout match {
      case Left(b) => b.toString
      case Right(t) => t.toString
    }
  }
  object `X-Deferred-Hash` extends ModeledCustomHeaderCompanion[`X-Deferred-Hash`] {
    override val name = "X-Deferred-Hash"
    override def parse(value: String) = Success(new `X-Deferred-Hash`(value))
  }
  final class `X-Deferred-Hash`(val hash: String) extends ModeledCustomHeader[`X-Deferred-Hash`] {
    def renderInRequests = false
    def renderInResponses = true
    override val companion = `X-Deferred-Hash`
    override def value: String = hash
  }

  trait DeferredStorage {
    def registerDeferredRequest(ctx: DeferredContext): DeferredContext
    def registerDeferredStatus(ctx: DeferredContext): DeferredContext
    def registerDeferredResult(ctx: DeferredContext): Future[DeferredContext]
    def getUserDeferredStatuses(userIdString: String): Iterable[DeferredContext]
    def cleanupDeferredRequests: Int
    //response field is not required to be filled
    def getDeferredRequest(hash: String, userIdString: String): Option[DeferredContext]
    def getDeferredResult(hash: String, userIdString: String): Option[HttpResponse]
    def getDeferredHttpRequest(hash: String, userIdString: String): Option[HttpRequest]
    /** Cleanup not finished requests after server restart */
    def onRestart(): Unit
  }

  import org.tresql._
  class DbDeferredStorage(conf: Config, exec: Execution, db: DbAccess, stats: ServerStatistics)
    extends DeferredStorage with AppFileStreamer[String] with AppConfig with DbAccessProvider {

    override lazy val appConfig = conf
    override def dbAccess = db
    import db._
    import stats._
    import exec._

    implicit private lazy val queryTimeout: QueryTimeout = DefaultQueryTimeout.getOrElse(QueryTimeout(10))

    override lazy val rootPath =
      conf.getString("deferred-requests.files.path").replaceAll("/+$", "")
    override lazy val file_info_table =
      Try(conf.getString("deferred-requests.file-info-table")).getOrElse("deferred_file_info")
    override lazy val file_body_info_table =
      Try(conf.getString("deferred-requests.file-body-info-table")).getOrElse("deferred_file_body_info")

    import DeferredControl.HttpMessageSerialization._
    def registerDeferredRequest(ctx: DeferredContext): DeferredContext = transaction {
      import ctx._
      /* TODO signature for exists() in tresql?
      val isDuplicate = tresql"""{exists(deferred_request[username = $userIdString & request_hash =
        $hash & status in ($DEFERRED_EXE, $DEFERRED_QUEUE)])}""".unique[Boolean]
      */
      val isDuplicate = tresql"""{ exists(deferred_request[username = $userIdString & request_hash =
        $hash & status in ($DEFERRED_EXE, $DEFERRED_QUEUE)]) }""".head[Boolean]
      if (isDuplicate)
        ctx.copy(status = DeferredExists) /*request already in exe phase or queued*/
      else {
        statsRegisterDeferredRequest
        implicit val usr = userIdString
        tresql"""-deferred_request[username = $userIdString & request_hash = $hash]"""
        tresql"""+deferred_request
                {username, priority, request_time, status, topic, request_hash, request}
                [$userIdString, $priority, $requestTime, $status, '', $hash, ${serializeHttpRequest(request)}]"""
        ctx
      }
    }

    def registerDeferredStatus(ctx: DeferredContext) = transaction {
      import ctx._
      tresql"=deferred_request[request_hash = $hash & username = $userIdString] {status, priority} [$status, $priority]"
      ctx
    }

    def registerDeferredResult(ctx: DeferredContext): Future[DeferredContext] = {
      import ctx._
      implicit val usr = userIdString
      serializeHttpResponse(this, result) match {
        case (bytes, Some(fif)) =>
          fif.map { fi =>
            transaction {
              statsRegisterDeferredResult
              tresql"""=deferred_request[$hash] {status, response_time, result, result_file_id, result_file_sha_256 }
               [$status, $responseTime, $bytes, ${fi.id}, ${fi.sha_256}]"""
              ctx
            }
          }
        case (bytes, None) =>
          Future.successful {
            transaction {
              statsRegisterDeferredResult
              tresql"""=deferred_request[$hash] {status, response_time, result } [$status, $responseTime, $bytes]"""
              ctx
            }
          }
      }
    }

    def getUserDeferredStatuses(userIdString: String): Iterable[DeferredContext] = dbUse {
      tresql"""deferred_request [username = $userIdString]
          { request, request_time, response_time, status, priority, request_hash }"""
        .list[java.io.InputStream, Timestamp, Timestamp, String, Int, String]
        .map(r => DeferredContext(userIdString, r._6,
          deserializeHttpMessage(r._1, None).asInstanceOf[HttpRequest],
          null, r._2, null, r._3, r._4, r._5))
    }

    def cleanupDeferredRequests: Int = transaction{
      val old = new java.sql.Timestamp(currentTime - deferredCleanupInterval.toMillis)
      tresql"=deferred_request[status in ($DEFERRED_OK, $DEFERRED_ERR) & response_time < $old] {status} [$DEFERRED_DEL]"
      tresql"deferred_request - [status = $DEFERRED_DEL]" match {
        case r: DeleteResult => r.count.get
        case _ => 0
      }
    }

    def getDeferredRequest(hash: String, userIdString: String) = dbUse {
      tresql"""deferred_request [request_hash = $hash & username = $userIdString]
          { request, request_time, response_time, status, priority }"""
        .headOption[java.io.InputStream, Timestamp, Timestamp, String, Int]
        .map(r => DeferredContext(userIdString, hash,
          deserializeHttpMessage(r._1, None).asInstanceOf[HttpRequest],
          null, r._2, null, r._3, r._4, r._5))
    }

    def getDeferredResult(hash: String, userIdString: String) = dbUse {
      tresql"""deferred_request [request_hash = $hash & username = $userIdString &
                 status in ($DEFERRED_OK, $DEFERRED_ERR)] { result, result_file_id, result_file_sha_256 }"""
        .headOption[java.io.InputStream, Long, String]
        .map {
          case (in, _, null) =>
            deserializeHttpMessage(in, None).asInstanceOf[HttpResponse]
          case (in, id, sha) =>
            deserializeHttpMessage(in, Some((this, userIdString, (id, sha)))).asInstanceOf[HttpResponse]
        }
    }

    def getDeferredHttpRequest(hash: String, userIdString: String) = dbUse {
      tresql"""deferred_request [request_hash = $hash & username = $userIdString] { request }"""
        .headOption[java.io.InputStream]
        .map(deserializeHttpMessage(_, None).asInstanceOf[HttpRequest])
    }

    import HttpMessageSerialization._
    val ServerRestartResponse = HttpResponse(StatusCodes.InternalServerError,
      entity = "Server restarted please repeat request")
    def onRestart(): Unit = {
      val responseTime = new Timestamp(currentTime)
      transaction {
        tresql"""=deferred_request[status in ($DEFERRED_EXE, $DEFERRED_QUEUE)] {status, response_time, result}
          [$DEFERRED_ERR, $responseTime, ${serializeHttpResponse(null, ServerRestartResponse)(null, null, null)._1}]"""
      }
    }
  }

  import akka.http.scaladsl.model.{HttpMessage, HttpEntity, HttpRequest, HttpResponse, HttpHeader,
  Uri, ContentType, HttpMethod, HttpProtocol, StatusCode}
 import akka.http.scaladsl.model.headers.RawHeader
 import akka.util.ByteString

  object HttpMessageSerialization {
  def serialize(obj: Serializable) = {
    val bos = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(bos)
    oos.writeObject(obj)
    oos.close
    bos.toByteArray
  }
  def deserialize(in: java.io.InputStream) = {
    val oin = new java.io.ObjectInputStream(in)
    val obj = oin.readObject
    oin.close
    obj
  }
  /* *********************************************************************************************
  HTTP request & response serialization, deserialization. Media types, headers not serializable :(
  ************************************************************************************************/
  def serializeHttpRequest(req: HttpRequest): Array[Byte] = req match {
      case HttpRequest(method, uri, headers, HttpEntity.Strict(contentType, content), protocol) =>
        serialize(
          ( method,
            uri,
            headers map (h => (h.name, h.value)),
            (contentType.value, content),
            protocol
          )
        )
      case x => sys.error(
        s"HttpMessage not serializable, check whether message entity is HttpEntity.Strict:$x")
  }
  def serializeHttpResponse(fs: AppFileStreamer[String], resp: HttpResponse)(
    implicit user: String, executor: ExecutionContextExecutor, materializer: akka.stream.Materializer): (Array[Byte], Option[Future[FileInfo]]) =
  resp match {
    case HttpResponse(status, headers, HttpEntity.Strict(contentType, content), protocol) =>
      serialize (
        ( status,
          headers map (h => (h.name, h.value)),
          (contentType.value, content),
          protocol
        )
      ) -> None
    case HttpResponse(status, headers, body, protocol) =>
      serialize (
        ( status,
          headers map (h => (h.name, h.value)),
          protocol
        )
      ) -> Some(body.dataBytes.runWith(fs.fileSink("deferred result", body.contentType.value)))
    case x => sys.error(
      s"HttpMessage not serializable, check whether message entity is HttpEntity.Strict:$x")
  }

  def deserializeHttpMessage(
                              in: java.io.InputStream,
                              fileBody: Option[(AppFileStreamer[String], String, (Long, String))]): HttpMessage =
    fileBody.map {
      case (fs, usr, (fid, sha)) =>
        deserialize(in) match {
          case (status: StatusCode, headers: scala.collection.immutable.Seq[(String, String)]@unchecked, protocol: HttpProtocol) =>
            fs.getFileInfo(fid, sha)(usr)
              .map { fi =>
                HttpResponse(status = status
                  , headers = headers map (h => RawHeader(h._1, h._2))
                  , entity =
                      HttpEntity.Default(
                        // This will always be MediaType.Binary, if 2nd param is true
                        // application/octet-stream as a fallback
                        MediaType.custom(Option(fi.content_type).filter(_ != null).filter(_ != "").getOrElse("application/octet-stream"), true)
                          .asInstanceOf[MediaType.Binary],
                        fi.size,
                        fi.source
                      )
                  , protocol = protocol
                )
              }.getOrElse(HttpResponse(status = StatusCodes.NotFound, entity = "Result not found"))
          case x => sys.error(s"Cannot deserialize http message: $x")
        }
    }.getOrElse {
      deserialize(in) match {
        case
          ( method: HttpMethod,
            uri: Uri,
            headers: scala.collection.immutable.Seq[(String, String)]@unchecked,
            (contentType: String, content: ByteString),
            protocol: HttpProtocol
          ) =>
          HttpRequest(
            method,
            uri,
            headers map (h => RawHeader(h._1, h._2)),
            HttpEntity.Strict(
              ContentType
                .parse(contentType)
                .fold(
                  err => sys.error(s"Unparsable content type: $err"),
                  identity
                ),
              content),
            protocol
          )
        case
          ( status: StatusCode,
            headers: scala.collection.immutable.Seq[(String, String)]@unchecked,
            (contentType: String, content: ByteString),
            protocol: HttpProtocol
          ) =>
          HttpResponse(
            status,
            headers map (h => RawHeader(h._1, h._2)),
            HttpEntity.Strict(
              ContentType
                .parse(contentType)
                .fold(
                  err => sys.error(s"Unparsable content type: $err"),
                  identity
                ),
              content
            ),
            protocol
          )
        case x => sys.error(s"Cannot deserialize http message: $x")
      }
    }
  }
}
