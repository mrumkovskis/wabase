package org.wabase

import com.typesafe.config.ConfigFactory
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.{ActorAttributes, Attributes, RestartSettings}
import org.apache.pekko.stream.scaladsl.{Flow, Keep, RestartSource, Sink, Source}
import org.tresql.{ArrayResult, Query, Resources, Result, RowLike, SingleValueResult}
import org.wabase.AppMetadata.Action
import org.wabase.AppQuerease.{ActionContext, Scope}
import org.wabase.msg._
import org.wabase.msg.MsgParams.MsgParamException

import org.apache.pekko.Done

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}
import scala.collection.immutable.ListMap
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

/** Messaging operations of view actions - msg read, msg send, msg admin calls, see docs/messaging.md.
  *
  * Tresql evaluation and action integration (scopes, steps, results) is here, provider independent parameter
  * conversion and stream stages are in [[org.wabase.msg.MsgParams]], [[org.wabase.msg.MsgStreams]].
  *
  * Message processing (per message tresql, foreach body) runs with resources of the operation - action's (or `db`
  * block's) connection, evaluator connection if there is none (explicit db view). Messaging result returned by
  * action keeps the resources until its stream terminates (see AppQuerease.processResult, [[withCleanup]]),
  * see docs/messaging.md, Database resources.
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
      val bindVars = scope.toBindeableMap(env)
      // parameters and per message tresql are evaluated with operation's resources, which are released after
      // the stream terminates if messaging result is returned by action
      val msgRes = qr.resourcesFactory.resources
      def eval(t: Action.Tresql) = evalMsgParam(t, bindVars, msgRes)
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
    * `restart` is evaluated before body, with operation scope. Other parameters (`filter`, then `to`, `key`,
    * `headers`, `using`) are evaluated for each element with [[elementScope]] bind variables, see [[sendMessage]].
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
    import qr.ec
    import context.env
    Future {
      val restart = MsgSendParams.sendRestart(
        evalMsgParam(op.restart, scope.toBindeableMap(env), qr.resourcesFactory.resources))
      val provider = qr.msgProviders.provider(op.providerName)
      (restart, provider, createSender(provider))
    }.flatMap { case (restart, provider, sender) =>
      // operation result is streamed, not collected - query result size limit does not apply
      val bodyQr = withoutResultSizeLimit
      def body(): Future[QuereaseResult] =
        doSteps(op.action, context.copy(stepName = "msg send"), Future.successful(Scope(Map(), parent = scope)))(bodyQr)
      // per element parameters are evaluated with action's resources - concurrently with reading of query result
      // rows on the same connection, JDBC driver must be thread safe
      val res = qr.resourcesFactory.resources
      val filter: ElementFilter = filterEvaluation(op.filter, _, scope, context, res)
      val message: ElementMessage = (el, keyName) =>
        Future(sendMessage(op, provider, el, elementScope(el, keyName, scope).toBindeableMap(env), res))
      body().flatMap { result =>
        restart match {
          case None => sendResult(result, message, sender, filter, context)
          case Some(settings) =>
            val sent = new AtomicLong
            restartMessaging(result, () => body(), settings, "msg send")(messagingSend(_, message, sender, filter, sent))
              .map(_ => sent.get)
        }
      }
    }.map(LongResult.apply)
  }

  /** Runs pipeline of messaging result, restartable - body result is evaluated again for each restart.
    * Parameter error ([[MsgParamException]] - e.g. invalid value, unknown codec, missing bind variable) is permanent -
    * it is not restarted, the operation fails with it.
    * @param first     body result of the first attempt (checked before restarting, so that wrong body is not restarted)
    * @param opName    operation name for error message
    * @param pipeline  send (and commit) pipeline of messaging result
    */
  private def restartMessaging(
    first: QuereaseResult,
    body: () => Future[QuereaseResult],
    settings: RestartSettings,
    opName: String,
  )(pipeline: MessagingResult => Source[Any, Control])(implicit qr: QuereaseResources): Future[Done] = {
    import qr.{as, ec}
    def messaging(res: QuereaseResult): MessagingResult = res match {
      case mr: MessagingResult => mr
      case CompatibleResult(mr: MessagingResult, _, _) => mr
      case DbResult(r, cleanup) => // db resources are released when messaging stream terminates
        try withCleanup(messaging(r), cleanup) catch { case NonFatal(e) => cleanup(Option(e)); throw e }
      case x => sys.error(
        s"$opName parameter 'restart' requires messaging result (msg read) of operation, got: ${x.getClass.getName}")
    }
    val firstResult = new AtomicReference(messaging(first))
    val permanentFailure = new AtomicReference[Throwable]
    RestartSource.onFailuresWithBackoff(settings) { () =>
      val mr = Option(firstResult.getAndSet(null)).map(Future.successful).getOrElse(body().map(messaging))
      Source.futureSource(mr.map(r => MsgStreams.stopOnTermination(pipeline(r))))
        .recoverWithRetries(1, { case e: MsgParamException => permanentFailure.set(e); Source.empty })
    }.runWith(Sink.ignore).flatMap { done =>
      Option(permanentFailure.get).map(Future.failed[Done]).getOrElse(Future.successful(done))
    }
  }

  /** Send (and commit) pipeline of messaging result, increments `sent` for each sent message.
    * Committables are committed with committer flow after messages are sent, in incoming message order,
    * `null` (filtered) messages are not sent, but committed. Commit failure fails the pipeline.
    */
  private def messagingSend(
    mr: MessagingResult,
    message: ElementMessage,
    sender: MsgSender,
    filter: ElementFilter,
    sent: AtomicLong,
  )(implicit qr: QuereaseResources): Source[Any, Control] = {
    import qr.ec
    mr.committer match {
      case Some(committer) =>
        mr.source
          .via(filter(mr.keyName))
          .mapAsync(1) {
            case CommittableMessage(null, c) => Future.successful((None, c))
            case CommittableMessage(v, c) => message(v, mr.keyName).map(m => (Some(m), c))
            case x => Future.failed(new IllegalStateException(s"Committable message expected, got: $x"))
          }
          .via(sender.optionalFlow[Committable])
          .map { case (result, c) => if (result.isDefined) sent.incrementAndGet(); c }
          .via(committer.flow)
      case None =>
        mr.source
          .via(filter(mr.keyName))
          .collect { case v if v != null => v }
          .mapAsync(1)(message(_, mr.keyName).map((_, NotUsed)))
          .via(sender.flow[NotUsed])
          .map(_ => sent.incrementAndGet())
    }
  }

  /** Sends each element of source operation (body) result to destinations, returns rows `{provider, topic, count}`
    * of sent message counts per destination and topic, in destination order, topics in order of the first message,
    * destination without sent messages - one row with `null` topic and 0 count ([[IteratorResult]]).
    * `restart` is evaluated before body. For each element destinations are evaluated in order - `filter`,
    * parameters (`to`, `key`, `headers`, `using`) and operation with [[elementScope]] bind variables (`null` or no
    * result - nothing is sent, rows - message per row), then messages are sent by destination stages in order,
    * committable (messaging result) is committed after the element is sent to all destinations.
    * Filters and destination operations run with action's resources - concurrently with reading of query result
    * rows on the same connection, JDBC driver must be thread safe.
    * Source and restart are as of [[doMsgSend]].
    */
  protected def doMsgMultiSend(
    op: Action.MsgMultiSend,
    scope: Scope,
    context: ActionContext,
  )(implicit qr: QuereaseResources): Future[QuereaseResult] = {
    import qr.{as, ec}
    import context.env
    Future {
      val restart = MsgSendParams.multiSendRestart(
        evalMsgParam(op.restart, scope.toBindeableMap(env), qr.resourcesFactory.resources))
      val destinations = op.destinations.steps.map {
        case (Action.Evaluation(_, _, s: Action.MsgSend), _) =>
          val provider = qr.msgProviders.provider(s.providerName)
          new MultiSendDestination(Option(s.providerName).getOrElse(MsgProviderConfig.defaultProviderName), s,
            provider, createSender(provider))
        case (_, src) => sys.error(s"msg multi send destination must be msg send operation, found: $src")
      }
      (restart, destinations)
    }.flatMap { case (restart, destinations) =>
      val bodyQr = withoutResultSizeLimit // source is streamed, not collected
      def body(): Future[QuereaseResult] = doSteps(op.action, context.copy(stepName = "msg multi send"),
        Future.successful(Scope(Map(), parent = scope)))(bodyQr)
      val msgRes = qr.resourcesFactory.resources
      def messagingPipeline(mr: MessagingResult): Source[Any, Control] = {
        val flow = multiSendFlow[Any](destinations, mr.keyName, scope, context, msgRes, qr)
        mr.committer match {
          case Some(committer) =>
            mr.source
              .map {
                case CommittableMessage(v, c) => (v, c)
                case x => sys.error(s"Committable message expected, got: $x")
              }
              .via(flow)
              .collect { case c: Committable => c }
              .via(committer.flow)
          case None => mr.source.map((_, NotUsed)).via(flow)
        }
      }
      def send(res: QuereaseResult): Future[Done] = res match {
        case DbResult(r, cleanup) => // Future.unit - cleanup also if send throws before returning future
          Future.unit.flatMap(_ => send(r)).andThen { case r => cleanup(r.failed.toOption) }
        case mr: MessagingResult => MsgStreams.run(messagingPipeline(mr), Sink.ignore).map(_ => Done)
        case CompatibleResult(mr: MessagingResult, _, _) => send(mr)
        case r =>
          elementSource(r, context).flatMap { src =>
            src.filter(v => v != null && v != NoResult).map((_, NotUsed))
              .via(multiSendFlow[Any](destinations, null, scope, context, msgRes, qr))
              .runWith(Sink.ignore)
          }
      }
      body().flatMap { res =>
        restart match {
          case None => send(res)
          case Some(settings) => restartMessaging(res, () => body(), settings, "msg multi send")(messagingPipeline(_))
        }
      }.map { _ =>
        IteratorResult(destinations.flatMap(_.rows).iterator)
      }
    }
  }

  /** Multi send pipeline - for each element `(value, P)` destination messages are evaluated, then sent by destination
    * stages in order, `P` (e.g. committable) is emitted after the element is sent to all destinations.
    * `null` value (filtered by foreach) is not sent, its `P` is passed through.
    * @param destQr  resources of destination operations
    */
  private def multiSendFlow[P](
    destinations: Seq[MultiSendDestination],
    keyName: String,
    scope: Scope,
    context: ActionContext,
    msgRes: Resources,
    destQr: QuereaseResources,
  )(implicit qr: QuereaseResources): Flow[(Any, P), P, NotUsed] = {
    import qr.ec
    type Msgs = Vector[Seq[OutMessage]] // messages per destination
    val noMessages: Msgs = Vector.fill(destinations.size)(Nil)
    val destContext = context.copy(stepName = "msg multi send to")
    @volatile var idx = 0L // mapAsync parallelism is 1
    def destinationMessages(d: MultiSendDestination, el: Any, elScope: Scope, bindVars: () => Map[String, Any])
      : Future[Seq[OutMessage]] = {
      val isSent =
        if (d.op.filter == null) Future.successful(true)
        else Future(MsgSendParams.isSent(evalMsgParam(d.op.filter, bindVars(), msgRes)))
      isSent.flatMap {
        case false => Future.successful(Nil)
        case true if d.op.action == null =>
          Future(sendMessage(d.op, d.provider, el, bindVars(), msgRes)).map(List(_))
        case true =>
          Future(sendParams(d.op, bindVars(), msgRes)).flatMap { params =>
            doSteps(d.op.action, destContext, Future.successful(elScope))(destQr)
              .flatMap(dataForNextStep(_, destContext, unwrapSingleValue = true)(destQr))
              .map(resultValues(_).map(transformedMessage(el, _, params, d.provider)))
          }
      }
    }
    def messages(el: Any): Future[Msgs] = {
      val i = idx
      idx += 1 // like foreach - filtered (null) elements are counted too
      if (el == null) Future.successful(noMessages)
      else {
        val elScope = elementScope(el, keyName, scope, i)
        lazy val bindVars = elScope.toBindeableMap(context.env) // filter bind variables, built once per element
        destinations.foldLeft(Future.successful(Vector.empty[Seq[OutMessage]])) { (msgsF, d) =>
          msgsF.flatMap(msgs => destinationMessages(d, el, elScope, () => bindVars).map(msgs :+ _))
        }
      }
    }
    val evaluated: Flow[(Any, P), (Msgs, P), NotUsed] =
      Flow[(Any, P)].mapAsync(1) { case (el, p) => messages(el).map(_ -> p) }
    destinations.zipWithIndex.foldLeft(evaluated) { case (flow, (d, i)) =>
      flow
        .mapConcat { case st@(msgs, _) => // message of element (or none), with flag - last message of element
          val ms = msgs(i)
          if (ms.isEmpty) List((Option.empty[OutMessage], (st, true)))
          else ms.zipWithIndex.map { case (m, j) => (Option(m), (st, j == ms.size - 1)) }.toList
        }
        .via(d.sender.optionalFlow[((Msgs, P), Boolean)])
        .map { case (result, st) => result.foreach(r => d.sent(r.topic)); st }
        .collect { case (st, true) => st }
    }.map(_._2)
  }

  /** Sender of provider with codecs from configuration - messages specify codecs of `using` parameter */
  private def createSender(provider: MsgProvider): MsgSender = provider.createSender(ConfigFactory.empty)

  /** msg send parameters `to`, `key`, `headers`, `using`, evaluated with bind variables of element.
    * Missing bind variable (e.g. operation variable without `..` prefix) is reported as [[MsgParamException]] -
    * parameter error, not restarted, see [[restartMessaging]].
    */
  private def sendParams(op: Action.MsgSend, bindVars: Map[String, Any], res: Resources): MsgSendParams = {
    val Seq(to, key, headers, usingCodecs) =
      try evalMsgParams(Seq(op.to, op.key, op.headers, op.using), bindVars, res) catch {
        case NonFatal(e) if causes(e).exists(_.isInstanceOf[org.tresql.MissingBindVariableException]) =>
          throw new MsgParamException(s"msg send parameter evaluation failed: ${e.getMessage}", e)
      }
    MsgSendParams.fromValues(to, key, headers, usingCodecs)
  }

  private def causes(e: Throwable): Iterator[Throwable] =
    Iterator.iterate(e)(_.getCause).takeWhile(_ != null).take(32)

  /** Outgoing message of element - msg send parameters are evaluated with element bind variables
    * (see [[sendParams]]), see [[outMessage]].
    */
  protected def sendMessage(
    op: Action.MsgSend,
    provider: MsgProvider,
    el: Any,
    bindVars: Map[String, Any],
    res: Resources,
  ): OutMessage = outMessage(el, sendParams(op, bindVars, res), provider)

  /** Outgoing message from destination operation result value - key of incoming message is retained
    * if `key` parameter is not specified, see [[outMessage]].
    */
  private def transformedMessage(el: Any, value: Any, params: MsgSendParams, provider: MsgProvider): OutMessage =
    (el, value) match {
      case (in: InMessage, v) if !v.isInstanceOf[InMessage] => outMessage(in.copy(value = v), params, provider)
      case (_, v) => outMessage(v, params, provider)
    }

  /** Outgoing message from result element, see [[MsgSendParams.outMessage]], with encoders of `using` codecs
    * (sender's - from configuration, if not specified).
    * [[InMessage]] (msg read without foreach) is forwarded - its value is sent, its key is retained
    * if `key` parameter is not specified.
    */
  protected def outMessage(value: Any, params: MsgSendParams, provider: MsgProvider): OutMessage = {
    val m = value match {
      case in: InMessage =>
        val m = params.outMessage(in.value)
        if (params.key.isEmpty) m.copy(key = in.key) else m
      case v => params.outMessage(v)
    }
    def codec(name: String) =
      if (name == null) null
      else try provider.codecs(name) catch {
        case NonFatal(e) => throw new MsgParamException(s"msg send parameter 'using': ${e.getMessage}", e)
      }
    m.copy(keyEncoder = codec(params.keyCodec), valueEncoder = codec(params.valueCodec))
  }

  private def sendResult(
    res: QuereaseResult,
    message: ElementMessage,
    sender: MsgSender,
    filter: ElementFilter,
    context: ActionContext,
  )(implicit qr: QuereaseResources): Future[Long] = {
    import qr.{as, ec}
    def send(values: Source[Any, _]): Future[Long] = values
      .filter(v => v != null && v != NoResult)
      .via(filter(null))
      .mapAsync(1)(message(_, null))
      .runWith(sender.sink)
    res match {
      case DbResult(r, cleanup) => // Future.unit - cleanup also if send throws before returning future
        Future.unit.flatMap(_ => sendResult(r, message, sender, filter, context))
          .andThen { case r => cleanup(r.failed.toOption) }
      case mr: MessagingResult =>
        val sent = new AtomicLong
        MsgStreams.run(messagingSend(mr, message, sender, filter, sent), Sink.ignore).map(_ => sent.get)
      case CompatibleResult(mr: MessagingResult, _, _) => sendResult(mr, message, sender, filter, context)
      case r => elementSource(r, context).flatMap(send)
    }
  }

  /** Elements of non messaging result - [[SourceResult]], query and iterator results are streamed
    * (see [[rowsSource]]), other results as [[dataForNextStep]] - rows, collection or single value.
    * Elements may be `null` or [[NoResult]] (from source result).
    */
  private def elementSource(res: QuereaseResult, context: ActionContext)(
    implicit qr: QuereaseResources): Future[Source[Any, _]] = {
    import qr.ec
    res match {
      case SourceResult(src, toBindableValue) => Future.successful(src.mapAsync(1)(toBindableValue))
      case r => rowsSource(r, context) match {
        case Some(src) => Future.successful(src)
        case None =>
          dataForNextStep(r, context, unwrapSingleValue = true).map { data =>
            Source(resultValues(data).toList) // stream elements must not be null
          }
      }
    }
  }

  /** Values of data - rows or collection elements, single value, empty for null or no result; nulls are excluded */
  private def resultValues(data: Any): Seq[Any] = (data match {
    case null | NoResult => Nil
    case s: Seq[_] => s
    case x => List(x)
  }).filter(v => v != null && v != NoResult)

  /** Streams rows of query result or elements of iterator result, so that large results are not collected into
    * memory. Values are the same as of [[dataForNextStep]] with `unwrapSingleValue` - row maps (compatible with view
    * of [[CompatibleResult]]), single row with single column is unwrapped to its value.
    * Query result is read ahead on IO dispatcher and closed on stream termination. Rows may be read concurrently with
    * processing of rows on the same connection (filter, destination operation) - JDBC driver must be thread safe.
    * Connection remains open until message send completes - send result (count) is returned after that, action's
    * resources are closed after the action.
    * @return None for other results
    */
  private def rowsSource(res: QuereaseResult, context: ActionContext)(
    implicit qr: QuereaseResources): Option[Source[Any, NotUsed]] = {
    import qr.ec
    def isRows(r: Result[_]) = !r.isInstanceOf[ArrayResult[_]] // DML, single value and array results are not rows
    def rows(r: Result[RowLike], toMap: RowLike => Map[String, Any]): Source[Any, NotUsed] = {
      val src = Source.fromIterator { () =>
        val it = r.map(toMap)
        if (!it.hasNext) Iterator.empty
        else {
          val first = it.next()
          if (!it.hasNext && first.size == 1) Iterator.single(first.head._2) else Iterator.single(first) ++ it
        }
      }
      src.addAttributes(Attributes(ActorAttributes.IODispatcher)).async
        .watchTermination() { (mat, done) => done.onComplete(_ => r.close()); mat }
    }
    res match {
      case TresqlResult(r) if isRows(r) => Some(rows(r, _.toMap))
      case CompatibleResult(TresqlResult(r), filter, _) if filter != null && isRows(r) =>
        val name = if (filter.name == "this") context.view.map(_.name).getOrElse(filter.name) else filter.name
        val vd = viewDef(name)
        Some(rows(r, toCompatibleMap(_, vd)))
      case IteratorResult(it) => Some(Source.fromIterator(() => it).mapAsync(1) {
        case r: QuereaseResult => dataForNextStep(r, context, unwrapSingleValue = true)
        case x => Future.successful(x)
      })
      case _ => None
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
    * @param res         operation's resources - tresql is evaluated on its connection, evaluator connection if none
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

  /** msg send `filter` evaluation for each element (see [[MsgSendParams.isSent]]) with [[elementScope]] bind
    * variables. Element which is not sent is dropped, committable message is retained with `null` message,
    * so that its committable is committed. Identity flow if filter is null.
    * @param res  operation's resources - tresql is evaluated on its connection, evaluator connection if none
    */
  protected def filterEvaluation(
    filter: Action.Tresql,
    keyName: String,
    scope: Scope,
    context: ActionContext,
    res: Resources,
  )(implicit qr: QuereaseResources): Flow[Any, Any, NotUsed] =
    if (filter == null) Flow[Any]
    else {
      import qr.ec
      def isSent(el: Any): Future[Boolean] = Future {
        MsgSendParams.isSent(evalMsgParam(filter, elementScope(el, keyName, scope).toBindeableMap(context.env), res))
      }
      Flow[Any].mapAsync(1) {
        case cm@CommittableMessage(null, _) => Future.successful(Some(cm))
        case cm@CommittableMessage(m, c) => isSent(m).map(s => Some(if (s) cm else CommittableMessage(null, c)))
        case m => isSent(m).map(s => if (s) Some(m) else None)
      }.collect { case Some(el) => el }
    }

  /** Bind variables of outgoing element - [[messageScope]] for incoming message, otherwise map fields or `value`,
    * like foreach item. Scope is not transparent - parent variables are accessible with `..` prefix.
    */
  protected def elementScope(el: Any, keyName: String, parent: Scope, idx: Long = -1): Scope = el match {
    case in: InMessage => messageScope(in, keyName, parent, idx = idx)
    case x =>
      val data = x match {
        case m: Map[String@unchecked, _] => m
        case v => Map(ValueVar -> v)
      }
      Scope(data, if (idx >= 0) Map(IdxVar -> idx) else Map(), parent, transparent = false)
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
    * Body runs with operation's resources (action's or `db` block's connection), see [[messageScope]] for message
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
      doSteps(op.action, ctx, Future.successful(itScope))
        .flatMap(dataForNextStep(_, ctx, unwrapSingleValue = true))
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

  /** Resources without query result size limit (`maxResultSize` 0), also for extra databases. `db` / `transaction`
    * resources are initialized from them, so the limit does not apply in nested db operations too.
    */
  private def withoutResultSizeLimit(implicit qr: QuereaseResources): QuereaseResources = {
    val res = qr.resourcesFactory.resources
    withResources(res.extraResources.keys
      .foldLeft(res.withMaxResultSize(0))((r, db) => r.withUpdatedExtra(db)(_.withMaxResultSize(0))))
  }

  /** Action's querease resources with other tresql resources */
  private def withResources(res: Resources)(implicit qr: QuereaseResources): QuereaseResources =
    new QuereaseResources()(
      qr.resourcesFactory.copy()(resources = res),
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
    evalMsgParams(Seq(t), bindVars, res).head

  /** Evaluates msg operation parameters, see [[evalMsgParam]], on one connection */
  private[wabase] def evalMsgParams(ts: Seq[Action.Tresql], bindVars: Map[String, Any], res: Resources): Seq[Any] =
    if (ts.forall(_ == null)) ts.map(_ => null)
    else useResourcesConnOrEvaluator(res, r => ts.map { t =>
      if (t == null) null
      else Query(t.tresql)(r.withParams(bindVars)) match {
        case SingleValueResult(v) => v
        case ar: ArrayResult[_] => ar.values.toList
        case rows: Result[_] => rows.toListOfMaps
      }
    })

  /** Messaging result which calls `cleanup` (releases db resources of operation - action, `db` block) when its
    * stream terminates - completes, fails (with the failure) or is cancelled, for messaging result consumed after
    * the operation has completed. Messaging result is consumed once (resources are not re-initialized), restart
    * evaluates operation again (see [[restartMessaging]]). Resources of a stream which is never materialized are
    * not released.
    */
  private[wabase] def withCleanup(mr: MessagingResult, cleanup: Option[Throwable] => Unit)(
    implicit ec: ExecutionContext): MessagingResult =
    mr.copy(source = mr.source.watchTermination() { (control, terminated) =>
      terminated.onComplete(t => cleanup(t.failed.toOption))
      control
    })

}

object AppQuereaseMsgOps {
  /** msg multi send destination - operation, provider, sender, sent message counts by topic */
  private class MultiSendDestination(
    val providerName: String,
    val op: Action.MsgSend,
    val provider: MsgProvider,
    val sender: MsgSender,
  ) {
    private val counts = scala.collection.mutable.LinkedHashMap.empty[String, Long]
    def sent(topic: String): Unit = synchronized(counts(topic) = counts.getOrElse(topic, 0L) + 1)
    /** Result rows `{provider, topic, count}` - per topic, `null` topic and 0 count if no messages are sent */
    def rows: List[Map[String, Any]] = synchronized {
      (if (counts.isEmpty) List((null, 0L)) else counts.toList).map { case (topic, count) =>
        ListMap("provider" -> providerName, "topic" -> topic, "count" -> count)
      }
    }
  }
  /** msg send filter flow by message key bind variable name (msg read `key`), see `filterEvaluation` */
  private type ElementFilter = String => Flow[Any, Any, NotUsed]
  /** msg send outgoing message of element by message key bind variable name (msg read `key`), see `sendMessage` */
  private type ElementMessage = (Any, String) => Future[OutMessage]
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
