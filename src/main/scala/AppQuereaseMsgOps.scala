package org.wabase

import com.typesafe.config.ConfigFactory
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.{ActorAttributes, Attributes, RestartSettings}
import org.apache.pekko.stream.scaladsl.{Flow, Keep, RestartSource, Sink, Source}
import org.tresql.{ArrayResult, Query, Resources, Result, RowLike, SingleValueResult}
import org.wabase.AppMetadata.Action
import org.wabase.AppQuerease.{ActionContext, Scope}
import org.wabase.msg._

import org.apache.pekko.Done

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}
import scala.collection.immutable.ListMap
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
    import qr.ec
    import context.env
    Future {
      val bindVars = scope.toBindeableMap(env)
      def eval(t: Action.Tresql) = evalMsgParam(t, bindVars, qr.resourcesFactory.resources)
      val params =
        MsgSendParams.fromValues(eval(op.to), eval(op.key), eval(op.headers), eval(op.using), eval(op.restart))
      (params, createSender(op.providerName, params))
    }.flatMap { case (params, sender) =>
      // operation result is streamed, not collected - query result size limit does not apply
      val bodyQr = withoutResultSizeLimit
      def body(): Future[QuereaseResult] =
        doSteps(op.action, context.copy(stepName = "msg send"), Future.successful(Scope(Map(), parent = scope)))(bodyQr)
      // filter is evaluated on evaluator connection - like other per message tresql, also for non messaging results,
      // so that action's connection is not used concurrently with streamed query result
      val msgRes = withoutConnections(qr.resourcesFactory.resources)
      val filter: ElementFilter = filterEvaluation(op.filter, _, scope, context, msgRes)
      body().flatMap { res =>
        params.restart match {
          case None => sendResult(res, params, sender, filter, context)
          case Some(settings) =>
            val sent = new AtomicLong
            restartMessaging(res, () => body(), settings, "msg send")(messagingSend(_, params, sender, filter, sent))
              .map(_ => sent.get)
        }
      }
    }.map(LongResult.apply)
  }

  /** Runs pipeline of messaging result, restartable - body result is evaluated again for each restart.
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
      case DbResult(r, cleanup) => cleanup(None); messaging(r)
      case x => sys.error(
        s"$opName parameter 'restart' requires messaging result (msg read) of operation, got: ${x.getClass.getName}")
    }
    val firstResult = new AtomicReference(messaging(first))
    RestartSource.onFailuresWithBackoff(settings) { () =>
      val mr = Option(firstResult.getAndSet(null)).map(Future.successful).getOrElse(body().map(messaging))
      Source.futureSource(mr.map(r => MsgStreams.stopOnTermination(pipeline(r))))
    }.runWith(Sink.ignore)
  }

  /** Send (and commit) pipeline of messaging result, increments `sent` for each sent message.
    * Committables are committed with committer flow after messages are sent, in incoming message order,
    * `null` (filtered) messages are not sent, but committed. Commit failure fails the pipeline.
    */
  private def messagingSend(
    mr: MessagingResult,
    params: MsgSendParams,
    sender: MsgSender,
    filter: ElementFilter,
    sent: AtomicLong,
  ): Source[Any, Control] = mr.committer match {
    case Some(committer) =>
      mr.source
        .via(filter(mr.keyName))
        .map {
          case CommittableMessage(v, c) => (Option(v).map(outMessage(_, params)), c)
          case x => sys.error(s"Committable message expected, got: $x")
        }
        .via(sender.optionalFlow[Committable])
        .map { case (result, c) => if (result.isDefined) sent.incrementAndGet(); c }
        .via(committer.flow)
    case None =>
      mr.source
        .via(filter(mr.keyName))
        .collect { case v if v != null => (outMessage(v, params), NotUsed) }
        .via(sender.flow[NotUsed])
        .map(_ => sent.incrementAndGet())
  }

  /** Sends each element of source operation (body) result to destinations, returns rows `{provider, topic, count}`
    * of sent message counts, one row per destination in destination order ([[IteratorResult]]).
    * Destination parameters and `restart` are evaluated before body. For each element destinations are evaluated in
    * order - `filter` (on evaluator connection) and operation with [[elementScope]] bind variables (`null` or no
    * result - nothing is sent, rows - message per row), then messages are sent by destination stages in order,
    * committable (messaging result) is committed after the element is sent to all destinations.
    * Destination operations of messaging result run with message processing resources (without action's
    * connections), of other results - with action's resources, query result rows are then read only when requested
    * (no read-ahead), so that the connection is not used concurrently.
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
      val bindVars = scope.toBindeableMap(env)
      def eval(t: Action.Tresql) = evalMsgParam(t, bindVars, qr.resourcesFactory.resources)
      val restart = MsgSendParams.multiSendRestart(eval(op.restart))
      val destinations = op.destinations.steps.map {
        case (Action.Evaluation(_, _, s: Action.MsgSend), _) =>
          val params = MsgSendParams.fromValues(eval(s.to), eval(s.key), eval(s.headers), eval(s.using))
          MultiSendDestination(Option(s.providerName).getOrElse(MsgProviderConfig.defaultProviderName), s, params,
            createSender(s.providerName, params), new AtomicLong)
        case (_, src) => sys.error(s"msg multi send destination must be msg send operation, found: $src")
      }
      (restart, destinations)
    }.flatMap { case (restart, destinations) =>
      val bodyQr = withoutResultSizeLimit // source is streamed, not collected
      def body(): Future[QuereaseResult] = doSteps(op.action, context.copy(stepName = "msg multi send"),
        Future.successful(Scope(Map(), parent = scope)))(bodyQr)
      val msgRes = withoutConnections(qr.resourcesFactory.resources)
      def messagingPipeline(mr: MessagingResult): Source[Any, Control] = {
        val flow = multiSendFlow[Any](destinations, mr.keyName, scope, context, msgRes, withResources(msgRes))
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
          // destination operations use action's connection - no read-ahead of query result
          val readAhead = destinations.forall(_.op.action == null)
          elementSource(r, context, readAhead).flatMap { src =>
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
        IteratorResult(destinations.map { d =>
          ListMap("provider" -> d.providerName, "topic" -> d.params.topic, "count" -> d.sent.get)
        }.iterator)
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
        case true if d.op.action == null => Future.successful(List(outMessage(el, d.params)))
        case true =>
          doSteps(d.op.action, destContext, Future.successful(elScope))(destQr)
            .flatMap(dataForNextStep(_, destContext, unwrapSingleValue = true)(destQr))
            .map(resultValues(_).map(transformedMessage(el, _, d.params)))
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
        .map { case (result, st) => if (result.isDefined) d.sent.incrementAndGet(); st }
        .collect { case (st, true) => st }
    }.map(_._2)
  }

  /** Sender of provider (default if name is null) with codecs of send parameters (from configuration if not
    * specified)
    */
  private def createSender(providerName: String, params: MsgSendParams)(implicit qr: QuereaseResources): MsgSender = {
    val provider = qr.msgProviders.provider(providerName)
    provider.createSender(ConfigFactory.empty,
      Option(params.keyCodec).map(provider.codecs(_)).orNull,
      Option(params.valueCodec).map(provider.codecs(_)).orNull,
    )
  }

  /** Outgoing message from destination operation result value - key of incoming message is retained
    * if `key` parameter is not specified, see [[outMessage]].
    */
  private def transformedMessage(el: Any, value: Any, params: MsgSendParams): OutMessage = (el, value) match {
    case (in: InMessage, v) if !v.isInstanceOf[InMessage] => outMessage(in.copy(value = v), params)
    case (_, v) => outMessage(v, params)
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
    filter: ElementFilter,
    context: ActionContext,
  )(implicit qr: QuereaseResources): Future[Long] = {
    import qr.{as, ec}
    def send(values: Source[Any, _]): Future[Long] = values
      .filter(v => v != null && v != NoResult)
      .via(filter(null))
      .map(outMessage(_, params))
      .runWith(sender.sink)
    res match {
      case DbResult(r, cleanup) => // Future.unit - cleanup also if send throws before returning future
        Future.unit.flatMap(_ => sendResult(r, params, sender, filter, context))
          .andThen { case r => cleanup(r.failed.toOption) }
      case mr: MessagingResult =>
        val sent = new AtomicLong
        MsgStreams.run(messagingSend(mr, params, sender, filter, sent), Sink.ignore).map(_ => sent.get)
      case CompatibleResult(mr: MessagingResult, _, _) => sendResult(mr, params, sender, filter, context)
      case r => elementSource(r, context).flatMap(send)
    }
  }

  /** Elements of non messaging result - [[SourceResult]], query and iterator results are streamed
    * (see [[rowsSource]]), other results as [[dataForNextStep]] - rows, collection or single value.
    * Elements may be `null` or [[NoResult]] (from source result).
    */
  private def elementSource(res: QuereaseResult, context: ActionContext, readAhead: Boolean = true)(
    implicit qr: QuereaseResources): Future[Source[Any, _]] = {
    import qr.ec
    res match {
      case SourceResult(src, toBindableValue) => Future.successful(src.mapAsync(1)(toBindableValue))
      case r => rowsSource(r, context, readAhead) match {
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
    * Query result is read on IO dispatcher and closed on stream termination. Connection remains open until message
    * send completes - send result (count) is returned after that, action's resources are closed after the action.
    * @param readAhead  read rows ahead asynchronously (on IO dispatcher), false if processing of rows uses the same
    *                   connection - rows are read when requested, connection is not used concurrently
    * @return None for other results
    */
  private def rowsSource(res: QuereaseResult, context: ActionContext, readAhead: Boolean = true)(
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
      (if (readAhead) src.addAttributes(Attributes(ActorAttributes.IODispatcher)).async else src)
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

  /** msg send `filter` evaluation for each element (see [[MsgSendParams.isSent]]) with [[elementScope]] bind
    * variables. Element which is not sent is dropped, committable message is retained with `null` message,
    * so that its committable is committed. Identity flow if filter is null.
    * @param res  resources without connections - tresql is evaluated on evaluator connection
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

  /** Resources without query result size limit (`maxResultSize` 0), also for extra databases. `db` / `transaction`
    * resources are initialized from them, so the limit does not apply in nested db operations too.
    */
  private def withoutResultSizeLimit(implicit qr: QuereaseResources): QuereaseResources = {
    val res = qr.resourcesFactory.resources
    withResources(res.extraResources.keys
      .foldLeft(res.withMaxResultSize(0))((r, db) => r.withUpdatedExtra(db)(_.withMaxResultSize(0))))
  }

  /** Resources for message processing - action's resources without connections, see [[withoutConnections]] */
  private def messageProcessingResources(implicit qr: QuereaseResources): QuereaseResources =
    withResources(withoutConnections(qr.resourcesFactory.resources))

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
  /** msg multi send destination - evaluated parameters, sender, sent message counter */
  private case class MultiSendDestination(
    providerName: String,
    op: Action.MsgSend,
    params: MsgSendParams,
    sender: MsgSender,
    sent: AtomicLong,
  )
  /** msg send filter flow by message key bind variable name (msg read `key`), see `filterEvaluation` */
  private type ElementFilter = String => Flow[Any, Any, NotUsed]
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
