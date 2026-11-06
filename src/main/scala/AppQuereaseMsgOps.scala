package org.wabase

import com.typesafe.config.ConfigFactory
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.RestartSettings
import org.apache.pekko.stream.scaladsl.{Flow, Keep, RestartSource, Sink, Source}
import org.tresql.{ArrayResult, Query, Resources, Result, SingleValueResult}
import org.wabase.AppMetadata.Action
import org.wabase.AppQuerease.{ActionContext, Scope}
import org.wabase.msg._

import org.apache.pekko.Done

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}
import scala.concurrent.{ExecutionContext, Future}

/** Messaging operations of view actions - msg read, msg send, msg admin calls, see docs/messaging.md.
  *
  * Tresql evaluation and action integration (scopes, steps, results) is here, provider independent parameter
  * conversion and stream stages are in [[org.wabase.msg.MsgParams]], [[org.wabase.msg.MsgStreams]].
  *
  * Message processing (per message tresql, foreach body) never uses action's db connection - it runs with action's
  * resources without connections, tresql is evaluated on evaluator connection, body accessing tables opens its own
  * db / transaction (see docs/messaging.md, Database resources).
  */
trait AppQuereaseMsgOps { this: AppQuerease =>
  import AppQuereaseMsgOps._

  /** Creates [[MessagingResult]] - messages are read when result is consumed (see [[runMessaging]]). */
  protected def doMsgRead(
    op: Action.MsgRead,
    scope: Scope,
    context: ActionContext,
  )(implicit qr: QuereaseResources): Future[QuereaseResult] = {
    import qr.{ec, msgProviders}
    import context.env
    Future {
      // parameters are evaluated while action is running - with action's resources
      val bindVars = scope.toBindeableMap(env)
      def eval(t: Action.Tresql) = evalMsgParam(t, bindVars, qr.resourcesFactory.resources)
      // message processing runs after action has completed, its connection is closed by then
      val msgRes = withoutConnections(qr.resourcesFactory.resources)
      val params = MsgReadParams.fromValues(
        group       = eval(op.group),
        from        = eval(op.from),
        timeout     = eval(op.timeout),
        committer   = eval(op.committer),
        key         = eval(op.key),
        usingCodecs = eval(op.using),
        atMostOnce  = eval(op.atMostOnce),
        restart     = eval(op.restart),
      )
      if (op.commitMetadata != null && params.committer.isEmpty)
        sys.error("msg read parameter 'commit_metadata' requires 'committer'")
      val provider = msgProviders.provider(op.providerName)
      val reader = provider.createReader(params.readerConfig,
        Option(params.keyCodec).map(provider.codecs(_)).orNull,
        Option(params.valueCodec).map(provider.codecs(_)).orNull,
      )
      val sub = params.subscription
      def readerSource: Source[Any, Control] =
        if (params.atMostOnce) reader.atMostOnceSource(sub)
        else if (params.committer.isEmpty) reader.source(sub)
        else if (op.commitMetadata == null) reader.committableSource(sub)
        else reader.committableSourceWithMetadata(sub, in => MsgReadParams.commitMetadata(evalMsgParam(
          op.commitMetadata, messageScope(in, params.keyName, scope).toBindeableMap(env), msgRes)))
      val source = params.restart.map(MsgStreams.restartOnFailure(_)(() => readerSource)).getOrElse(readerSource)
      // after termination messages are dropped (drain mode) - `to` is not evaluated for them
      val terminated = new AtomicBoolean
      MessagingResult(
        source
          .via(toEvaluation(op.to, params.keyName, scope, context, msgRes, terminated))
          .via(MsgStreams.termination[Any](params.within, params.idle, drain = params.committer.isDefined,
            onStop = () => terminated.set(true))),
        params.committer.map(_ => provider.createCommitter(params.committerConfig)),
        params.keyName,
      )
    }
  }

  /** Sends result of operation (body) as messages, returns sent message count ([[LongResult]]).
    * Parameters are evaluated before body, with operation scope.
    * Result elements are sent as messages - [[MessagingResult]] (committable - committed after sending, in incoming
    * message order), [[SourceResult]], rows, collection; single value (map, scalar) - one message, `null` elements
    * are not sent. See [[outMessage]] for message creation.
    * With `restart` parameter body must return [[MessagingResult]] - on failure (reading, processing, sending,
    * committing) whole pipeline is restarted with backoff, body is evaluated again (new consumer continues from
    * committed offset). Sent message count includes messages sent again after restarts.
    */
  protected def doMsgSend(
    op: Action.MsgSend,
    scope: Scope,
    context: ActionContext,
  )(implicit qr: QuereaseResources): Future[QuereaseResult] = {
    import qr.{ec, msgProviders}
    import context.env
    Future {
      val bindVars = scope.toBindeableMap(env)
      def eval(t: Action.Tresql) = evalMsgParam(t, bindVars, qr.resourcesFactory.resources)
      val params =
        MsgSendParams.fromValues(eval(op.to), eval(op.key), eval(op.headers), eval(op.using), eval(op.restart))
      val provider = msgProviders.provider(op.providerName)
      (params, provider.createSender(ConfigFactory.empty,
        Option(params.keyCodec).map(provider.codecs(_)).orNull,
        Option(params.valueCodec).map(provider.codecs(_)).orNull,
      ))
    }.flatMap { case (params, sender) =>
      def body(): Future[QuereaseResult] =
        doSteps(op.action, context.copy(stepName = "msg send"), Future.successful(Scope(Map(), parent = scope)))
      body().flatMap { res =>
        params.restart match {
          case None => sendResult(res, params, sender, context)
          case Some(settings) => sendWithRestart(res, () => body(), settings, params, sender)
        }
      }
    }.map(LongResult.apply)
  }

  /** Sends messaging result in restartable pipeline - body result is evaluated again for each restart.
    * @param first  body result of the first attempt (checked before restarting, so that wrong body is not restarted)
    */
  private def sendWithRestart(
    first: QuereaseResult,
    body: () => Future[QuereaseResult],
    settings: RestartSettings,
    params: MsgSendParams,
    sender: MsgSender,
  )(implicit qr: QuereaseResources): Future[Long] = {
    import qr.{as, ec}
    def messaging(res: QuereaseResult): MessagingResult = res match {
      case mr: MessagingResult => mr
      case DbResult(r, cleanup) => cleanup(None); messaging(r)
      case x => sys.error(
        s"msg send parameter 'restart' requires messaging result (msg read) of operation, got: ${x.getClass.getName}")
    }
    val firstResult = new AtomicReference(messaging(first))
    val sent = new AtomicLong
    RestartSource.onFailuresWithBackoff(settings) { () =>
      val mr = Option(firstResult.getAndSet(null)).map(Future.successful).getOrElse(body().map(messaging))
      Source.futureSource(mr.map(r => MsgStreams.stopOnTermination(messagingSend(r, params, sender, sent))))
    }.runWith(Sink.ignore).map(_ => sent.get)
  }

  /** Send (and commit) pipeline of messaging result, increments `sent` for each sent message.
    * Committables are committed with committer flow after messages are sent, in incoming message order,
    * `null` (filtered) messages are not sent, but committed. Commit failure fails the pipeline.
    */
  private def messagingSend(
    mr: MessagingResult,
    params: MsgSendParams,
    sender: MsgSender,
    sent: AtomicLong,
  ): Source[Any, Control] = mr.committer match {
    case Some(committer) =>
      mr.source
        .map {
          case CommittableMessage(v, c) => (Option(v).map(outMessage(_, params)), c)
          case x => sys.error(s"Committable message expected, got: $x")
        }
        .via(sender.optionalFlow[Committable])
        .map { case (result, c) => if (result.isDefined) sent.incrementAndGet(); c }
        .via(committer.flow)
    case None =>
      mr.source
        .collect { case v if v != null => (outMessage(v, params), NotUsed) }
        .via(sender.flow[NotUsed])
        .map(_ => sent.incrementAndGet())
  }

  /** Outgoing message from result element, see [[MsgSendParams.outMessage]].
    * [[InMessage]] (msg read without foreach) is forwarded - its value is sent, its key is retained
    * if `key` parameter is not specified.
    */
  protected def outMessage(value: Any, params: MsgSendParams): OutMessage = value match {
    case in: InMessage =>
      val m = params.outMessage(in.value)
      if (params.key.isEmpty) m.copy(key = in.key) else m
    case v => params.outMessage(v)
  }

  private def sendResult(
    res: QuereaseResult,
    params: MsgSendParams,
    sender: MsgSender,
    context: ActionContext,
  )(implicit qr: QuereaseResources): Future[Long] = {
    import qr.{as, ec}
    def out(v: Any) = outMessage(v, params)
    res match {
      case DbResult(r, cleanup) =>
        sendResult(r, params, sender, context).andThen { case r => cleanup(r.failed.toOption) }
      case mr: MessagingResult =>
        val sent = new AtomicLong
        MsgStreams.run(messagingSend(mr, params, sender, sent), Sink.ignore).map(_ => sent.get)
      case SourceResult(src, toBindableValue) =>
        src.mapAsync(1)(toBindableValue).filter(v => v != null && v != NoResult).map(out).runWith(sender.sink)
      case r =>
        dataForNextStep(r, context, unwrapSingleValue = true).flatMap { data =>
          val values = data match {
            case null | NoResult => Nil
            case s: Seq[_] => s
            case x => List(x)
          }
          Source(values.filter(_ != null).toList).map(out).runWith(sender.sink)
        }
    }
  }

  /** Calls provider admin ([[MsgAdmin]]), returns rows ([[IteratorResult]]), see [[MsgAdminParams]] for argument
    * and result row columns.
    */
  protected def doMsgAdmin(
    op: Action.MsgAdmin,
    scope: Scope,
    context: ActionContext,
  )(implicit qr: QuereaseResources): Future[QuereaseResult] = {
    import qr.{ec, msgProviders}
    import context.env
    import Action.{MsgAdmin => Cmd}
    import MsgAdminParams._
    Future {
      val args = evalMsgParam(op.args, scope.toBindeableMap(env), qr.resourcesFactory.resources)
      (msgProviders.provider(op.providerName).admin, args)
    }.flatMap { case (admin, args) =>
      val cmd = op.command
      cmd match {
        case Cmd.Topics => admin.topics.map(ts => partitionRows(ts.values.flatten))
        case Cmd.TopicPartitions =>
          Future.traverse(topics(cmd, args))(admin.partitionsFor).map(ps => partitionRows(ps.flatten))
        case Cmd.BeginningOffsets => admin.beginningOffsets(topicPartitions(cmd, args)).map(offsetRows)
        case Cmd.EndOffsets => admin.endOffsets(topicPartitions(cmd, args)).map(offsetRows)
        case Cmd.CommittedOffsets =>
          Future.traverse(groupPartitions(cmd, args).toList) { case (group, partitions) =>
            admin.committedOffsets(group, partitions).map(group -> _)
          }.map(os => committedOffsetRows(os.toMap))
        case x => sys.error(s"Unknown msg admin command: $x")
      }
    }.map(rows => IteratorResult(rows.iterator))
  }

  /** Maps message to `(message, isLast)`, `isLast` is result of `to` evaluation with message bind variables.
    * @param res         resources without connections - tresql is evaluated on evaluator connection
    * @param terminated  set when stream is terminated - following messages are dropped, `to` is not evaluated
    */
  protected def toEvaluation(
    to: Action.Tresql,
    keyName: String,
    scope: Scope,
    context: ActionContext,
    res: Resources,
    terminated: AtomicBoolean,
  )(implicit qr: QuereaseResources): Flow[Any, (Any, Boolean), NotUsed] =
    if (to == null) Flow[Any].map(el => (el, false))
    else Flow[Any].mapAsync(1) { el =>
      if (terminated.get) Future.successful((el, false))
      else Future {
        val bindVars = messageScope(inMessage(el), keyName, scope).toBindeableMap(context.env)
        (el, MsgReadParams.isLast(evalMsgParam(to, bindVars, res)))
      }(qr.ec)
    }

  /** Message bind variables - value under `itVar` if specified (foreach `<var> in`), otherwise map value fields or
    * `value`, metadata ([[InMessage.metadata]]) as `__metadata`, message key as `keyName` if not null and value does
    * not contain it, foreach item index as `__idx` if not negative.
    * Like foreach item scope, scope is not transparent - parent variables are accessible with `..` prefix.
    */
  protected def messageScope(
    in: InMessage,
    keyName: String,
    parent: Scope,
    itVar: String = null,
    idx: Long = -1,
  ): Scope = {
    val data: Map[String, Any] =
      if (itVar != null) Map(itVar -> in.value)
      else in.value match {
        case m: Map[String@unchecked, _] => m
        case v => Map(ValueVar -> v)
      }
    val withKey = if (keyName != null && !data.contains(keyName)) data + (keyName -> in.key) else data
    val initVars = Map[String, Any](MetadataVar -> in.metadata) ++ (if (idx >= 0) Map(IdxVar -> idx) else Map())
    Scope(withKey, initVars, parent, transparent = false)
  }

  /** Foreach on [[MessagingResult]] - body is executed for each message, result is MessagingResult of body results
    * (committable - `CommittableMessage(body result, committable)`), or fold result if fold op is specified.
    * Body runs with message processing resources (without action's connections), see [[messageScope]] for message
    * bind variables (elements which are not [[InMessage]] - result of previous foreach - are bound like foreach items).
    * Body result `null` or no result filters message out - it is not emitted, but committable is still committed.
    */
  private[wabase] def doForeachMessaging(
    op: Action.Foreach,
    mr: MessagingResult,
    scope: Scope,
    context: ActionContext,
  )(implicit qr: QuereaseResources): Future[QuereaseResult] = {
    import qr.ec
    val msgQr = messageProcessingResources
    val ctx = context.copy(stepName = "foreach")
    @volatile var idx = 0L // mapAsync parallelism is 1
    def process(el: Any): Future[Any] = {
      val itScope = el match {
        case in: InMessage => messageScope(in, mr.keyName, scope, op.itVar, idx)
        case x =>
          val data = if (op.itVar != null) Map(op.itVar -> x) else x match {
            case m: Map[String@unchecked, _] => m
            case v => Map(ValueVar -> v)
          }
          Scope(data, Map(IdxVar -> idx), scope, transparent = false)
      }
      idx += 1
      doSteps(op.action, ctx, Future.successful(itScope))(msgQr)
        .flatMap(dataForNextStep(_, ctx, unwrapSingleValue = true)(msgQr))
        .map {
          case NoResult => null
          case x => x
        }
    }
    val source = mr.source
      .mapAsync(1) {
        case CommittableMessage(m, c) => process(m).map(v => Some(CommittableMessage(v, c))) // null v - committed
        case m => process(m).map(Option(_))                                               // null - filtered
      }
      .collect { case Some(el) => el }
    val res = mr.copy(source = source)
    if (op.foldOp == null) Future.successful(res)
    else runMessaging(res, Sink.foldAsync[Any, Any](scope(op.foldOp.resVar)) { (acc, el) =>
      doActionOp(op.foldOp.op, Scope(Map(op.foldOp.resVar -> acc, op.foldOp.elVar -> el)), context)
        .flatMap(dataForNextStep(_, context, unwrapSingleValue = true))
    }).map(AnyResult.apply)
  }

  /** Resources for message processing - action's resources without connections, see [[withoutConnections]] */
  private def messageProcessingResources(implicit qr: QuereaseResources): QuereaseResources =
    new QuereaseResources()(
      qr.resourcesFactory.copy()(resources = withoutConnections(qr.resourcesFactory.resources)),
      qr.ec, qr.as, qr.httpReq, qr.qio, qr.fileStreamers, qr.httpClients, qr.msgProviders, qr.parametersProvider,
      qr.logger)

  /** Runs messaging result into sink of element values, stops consumer after stream completes, see [[MsgStreams.run]].
    * [[InMessage]] is converted to map ([[InMessage.toMap]]), committable messages are committed with result's
    * committer, `null` values (filtered messages) are committed, but not passed to sink.
    * Returned future completes after all messages are committed.
    */
  private[wabase] def runMessaging[M](mr: MessagingResult, sink: Sink[Any, Future[M]])(implicit
    qr: QuereaseResources): Future[M] = {
    import qr.as
    def value(v: Any) = messageValue(v)
    val valueSink: Sink[Any, Future[M]] = mr.committer match {
      case None => Flow[Any].map(value).filter(_ != null).toMat(sink)(Keep.right)
      case Some(committer) =>
        Flow[Any].map {
          case CommittableMessage(m, c) => (value(m), c)
          case x => sys.error(s"Committable message expected, got: $x")
        }
          .alsoToMat(Flow[(Any, Committable)].map(_._2).toMat(committer.sink)(Keep.right))(Keep.right)
          .collect { case (v, _) if v != null => v } // stream elements must not be null
          .toMat(sink)(Keep.both)
          .mapMaterializedValue { case (committed, result) =>
            committed.flatMap(_ => result)(qr.ec)
          }
    }
    MsgStreams.run(mr.source, valueSink)
  }

  /** Source of messaging result element values for streams run by others (http entity - SSE marshaller):
    * [[InMessage]] is converted to map ([[InMessage.toMap]]), committable messages are committed with result's
    * committer as they are emitted (not acknowledged by the receiver), `null` values (filtered messages) are
    * committed, but not emitted. Consumer is stopped ([[Control.stop]]) after stream terminates (completes, fails
    * or is cancelled, e.g. http client disconnects) and pending commits complete.
    */
  def messagingSource(mr: MessagingResult)(implicit ec: ExecutionContext): Source[Any, NotUsed] = {
    val values: Source[Any, (Control, Future[Any])] = mr.committer match {
      case None =>
        mr.source.map(messageValue).filter(_ != null).mapMaterializedValue(c => (c, Future.successful(Done)))
      case Some(committer) =>
        mr.source
          .map {
            case CommittableMessage(m, c) => (messageValue(m), c)
            case x => sys.error(s"Committable message expected, got: $x")
          }
          .alsoToMat(Flow[(Any, Committable)].map(_._2).toMat(committer.sink)(Keep.right))(Keep.both)
          .collect { case (v, _) if v != null => v } // stream elements must not be null
    }
    values.watchTermination()(Keep.both).mapMaterializedValue { case ((control, committed), terminated) =>
      terminated.transformWith(_ => committed).onComplete(_ => control.stop())
      NotUsed
    }
  }

  private def messageValue(v: Any): Any = v match {
    case in: InMessage => in.toMap
    case x => x
  }

  /** Evaluates msg operation parameter tresql - rows, single value or null if tresql is null.
    * Tresql is evaluated on resources connection, on evaluator connection if there is none.
    */
  private[wabase] def evalMsgParam(t: Action.Tresql, bindVars: Map[String, Any], res: Resources): Any =
    if (t == null) null
    else useResourcesConnOrEvaluator(res, r => Query(t.tresql)(r.withParams(bindVars)) match {
      case SingleValueResult(v) => v
      case ar: ArrayResult[_] => ar.values.toList
      case rows: Result[_] => rows.toListOfMaps
    })

  /** Resources for message processing - action's resources without db connections. Message processing runs after
    * action has completed (MessagingResult is not closeable), action's connection (if view is not explicit db)
    * is closed by then. Tresql is evaluated on evaluator connection, db / transaction op opens its own connection.
    */
  private def withoutConnections(res: Resources): Resources =
    res.extraResources.keys.foldLeft(res.withConn(null))((r, db) => r.withUpdatedExtra(db)(_.withConn(null)))

}

object AppQuereaseMsgOps {
  /** Message bind variable of non map message value */
  val ValueVar = "value"
  /** Message bind variable of message metadata, see [[InMessage.metadata]] */
  val MetadataVar = "__metadata"
  /** Foreach item index bind variable */
  val IdxVar = "__idx"

  private def inMessage(el: Any): InMessage = el match {
    case CommittableMessage(m: InMessage, _) => m
    case m: InMessage => m
    case x => sys.error(s"Incoming message expected, got: $x")
  }
}
