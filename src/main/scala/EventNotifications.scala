package org.wabase

import org.apache.pekko.http.scaladsl.server.directives.WebSocketDirectives
import org.apache.pekko.http.scaladsl.model.ws.{Message, TextMessage}
import org.apache.pekko.stream.{ActorAttributes, OverflowStrategy, Supervision}
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Sink, Source}
import org.apache.pekko.actor.{Actor, ActorNotFound, ActorRef, ActorSystem, Props, Terminated}
import DeferredControl._
import org.apache.pekko.http.scaladsl.marshalling.Marshal
import org.apache.pekko.http.scaladsl.marshalling.sse.EventStreamMarshalling
import org.apache.pekko.http.scaladsl.model.headers.{CacheDirectives, RawHeader, `Cache-Control`}
import org.apache.pekko.http.scaladsl.model.{AttributeKeys, HttpRequest, HttpResponse}
import org.apache.pekko.http.scaladsl.model.sse.ServerSentEvent
import org.apache.pekko.http.scaladsl.server.{Directives, Route}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

trait ServerNotifications extends EventStreamMarshalling with WebSocketDirectives {
  this: ServerNotifications.InitialEventsPublisher
    with Execution
    with Loggable =>

    // start event subscriber watcher actor
    actorSystem
      .actorSelection(actorSystem / ServerNotifications.SubscriberWatcherActorName)
      .resolveOne(1.second)
      .onComplete {
        case Success(_) => logger.info(s"Subscriber watcher already exists")
        case Failure(_: ActorNotFound) =>
          actorSystem.actorOf(Props(classOf[ServerNotifications.EventSubscriberWatcher]),
            ServerNotifications.SubscriberWatcherActorName)
        case Failure(e) => logger.error("Unable to start subscriber watcher actor", e)
      }

    /* ***********************
    *** Event notification ***
    **************************/
    def serverSideEventAction(userIdString: String): Route = Directives.complete {
      ServerNotifications.subscribeToEvents(
        bus => act => bus.subscribe(act, ServerNotifications.UserAddresseeMsg(userIdString)),
        _ => publishInitialEvents(userIdString)
      )(actorSystem)
    }
    /**
      * Consider [[serverSideEventAction]] instead
      * */
    def wsNotificationsAction(userIdString: String) = {
      handleWebSocketMessages(ServerNotifications.subscribeToWsMessages(
        bus => act => bus.subscribe(act, ServerNotifications.UserAddresseeMsg(userIdString)),
        _ => publishInitialEvents(userIdString)
      )(actorSystem))
    }
    def publishUserEvents(user: String, events: Iterable[Any]) = {
      events.foreach(publishUserEvent(user, _))
    }
    def publishUserEvent(user: String, event: Any) = {
      import ServerNotifications._
      val addressee = UserAddresseeMsg(user)
      EventBus.publish(EventMessage(addressee, event))
    }
    /** Return all actual user events client through web socket should be notified about.
    Is called when web socket connection is established. Must be overrided by subclasses. */
    def getActualUserEvents(user: String): Iterable[Any] = Nil
    /* End of event notification */
}

object ServerNotifications extends EventStreamMarshalling with Loggable {

  def createServerEvent(event: Any): ServerSentEvent = event match {
    case ctx: DeferredContext =>
      val data =
        Map(ctx.hash -> Map("status" -> ctx.status, "time" -> Option(ctx.responseTime).getOrElse(ctx.requestTime)))
      new ServerSentEvent(data = ResultEncoder.encodeAnyToJsonString(data))
    case x =>
      val data = x match {
        case s: String => s
        case x => ResultEncoder.encodeAnyToJsonString(x)
      }
      new ServerSentEvent(data = data)
  }

  private val ServerEventFunction = config.getString("app.server-notifications.event-function")
  val SubscriberWatcherActorName = config.getString("app.server-notifications.event-subscriber-watcher-actor-name")

  private def invokeCreateServerEventFunction(event: Any)(as: ActorSystem) = {
    invokeFunction(ServerEventFunction,
      Seq((classOf[ActorSystem], () => as)),
      { case (_, idx) if idx == 0 => event }: InvocationParameterFun
    )(as.dispatcher) match {
      case e: ServerSentEvent => e
      case x => sys.error(s"ServerSentEvent type expected but got: '$x' of type ${x.getClass}")
    }
  }

  protected def serverEventsSource(as: ActorSystem): Source[ServerSentEvent, ActorRef] = {
    Source
      .actorRef[Any](PartialFunction.empty, PartialFunction.empty, 16, OverflowStrategy.dropTail)
      .map(invokeCreateServerEventFunction(_)(as))
  }

  protected def wsNotificationGraph(as: ActorSystem): Flow[Message, Message, ActorRef] = {
    Flow.fromSinkAndSourceCoupledMat(
        Sink.ignore, // ignore incoming messages from the client
        serverEventsSource(as)
      ) (Keep.right)
      .map { e => TextMessage.Strict(e.data) }
      .withAttributes(ActorAttributes.supervisionStrategy{
        case ex: Exception =>
          logger.error("WsNotificationGraph crashed", ex)
          Supervision.Stop
      })
  }

  def subscribe(
    act: ActorRef,
    subscriptionFun: EventBus => ActorRef => Unit,
    initialPublications: EventBus => Unit,
  )(as: ActorSystem) = {
    // wait for the result here since this function is called in mapMaterializedValue and in the case of
    // Failure it will probably be silently omitted
    val watcher = Await.result(
      as.actorSelection(as / SubscriberWatcherActorName).resolveOne(1.second),
      1.second
    )
    watcher ! ServerNotifications.EventSubscriberActorMsg(
      act, subscriptionFun, initialPublications)
  }


  def subscribeToEvents(
    subscriptionFun: EventBus => ActorRef => Unit,
    initialPublications: EventBus => Unit,
  )(as: ActorSystem): Source[ServerSentEvent, Any] = {
    serverEventsSource(as).mapMaterializedValue {
      subscribe(_, subscriptionFun, initialPublications)(as)
    }
  }

  def subscribeToEventsAndListen(
    subscriptionFun: EventBus => ActorRef => Unit,
    initialPublications: EventBus => Unit,
  )(as: ActorSystem, req: HttpRequest): Future[HttpResponse] = {
    val dataSrc = subscribeToEvents(subscriptionFun, initialPublications)(as)
    implicit val ec: ExecutionContext = as.dispatcher
    Marshal(dataSrc).toResponseFor(req)
      .map(_.withHeaders(
        `Cache-Control`(CacheDirectives.`no-cache`),
         RawHeader("X-Accel-Buffering", "no"),
      ))
  }

  def subscribeToWsMessages(
    subscriptionFun: EventBus => ActorRef => Unit,
    initialPublications: EventBus => Unit,
  )(as: ActorSystem): Flow[Message, Message, Any] = {
     wsNotificationGraph(as).mapMaterializedValue(subscribe(_, subscriptionFun, initialPublications)(as))
  }

  def subscribeToWsMessagesAndListen(
    subscriptionFun: EventBus => ActorRef => Unit,
    initialPublications: EventBus => Unit,
  )(as: ActorSystem, req: HttpRequest): HttpResponse = {
    val upgrade = req.attribute(AttributeKeys.webSocketUpgrade)
      .getOrElse(sys.error("Expected web request web socket upgrade"))
    upgrade.handleMessages(
      subscribeToWsMessages(subscriptionFun, initialPublications)(as)
    )
  }

  def publish(publicationFun: EventBus => Unit): Unit = {
    publicationFun(EventBus)
  }

  def publishMessages(messages: EventMessage*): Unit = {
    publish(bus => messages.foreach(bus.publish))
  }

  def publishEvent(topic: String, value: String): Unit = {
    ServerNotifications.publish { _.publish(EventMessage(topic, value)) }
  }

  /** Publishes events to newly created websocket */
  trait InitialEventsPublisher {
    def publishInitialEvents(userIdString: String): Unit
  }

  trait NoInitialEvents extends InitialEventsPublisher {
    def publishInitialEvents(user: String): Unit = {}
  }

  /** Publishes app version and deferred events status info */
  trait DefaultInitialEventsPublisher extends InitialEventsPublisher {
    this: ServerNotifications
     with AppVersion
     with DeferredStatusPublisher
     with Execution
     with Loggable =>
    def publishInitialEvents(user: String): Unit = {
      publishUserEvent(user, ResultEncoder.encodeAnyToJsonString(Map("version" -> appVersion)))
      publishUserDeferredStatuses(user)
      publishUserEvents(user, getActualUserEvents(user))
    }
  }

  trait Addressee
  case class UserAddresseeMsg(user: String) extends Addressee
  case class EventSubscriberActorMsg(
    actor: ActorRef,
    subscriptions: EventBus => ActorRef => Unit,
    initialPublications: EventBus => Unit,
  )

  class EventSubscriberWatcher extends Actor with org.apache.pekko.actor.ActorLogging {
    override def preStart() = {
      logger.info(s"EventSubscriberWatcher actor started")
    }
    override def receive = {
      case EventSubscriberActorMsg(actor, subscriptions, initialPublications) =>
        context watch actor
        subscriptions(EventBus)(actor)
        initialPublications(EventBus)
      case Terminated(actor) =>
        EventBus.unsubscribe(actor)
        context unwatch actor
    }
    override def postStop() = {
      logger.info(s"EventSubscriberWatcher actor stopped")
    }
  }

  case class MsgEnvelope(topic: String, payload: Any)
  case class DeferredNotification(value: Any)

  def publish(msgEnvelope: MsgEnvelope)(implicit serverNotif: ServerNotifications): Unit = {
    serverNotif.publishUserEvent(msgEnvelope.topic, msgEnvelope.payload match {
      case DeferredNotification(value) => ResultEncoder.encodeAnyToJsonString(value)
      case x => x
    })
  }
}
