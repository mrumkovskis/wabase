package org.wabase

import akka.http.scaladsl.server.directives.WebSocketDirectives
import akka.http.scaladsl.model.ws.TextMessage
import akka.stream.{ActorAttributes, OverflowStrategy, Supervision}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.actor.{Actor, ActorRef, Props, Terminated}
import spray.json._
import DefaultJsonProtocol._
import DeferredControl._
import akka.http.scaladsl.marshalling.sse.EventStreamMarshalling
import akka.http.scaladsl.model.sse.ServerSentEvent
import akka.http.scaladsl.server.{Directives, Route}

trait ServerNotifications extends EventStreamMarshalling with WebSocketDirectives {
  this: ServerNotifications.InitialEventsPublisher
    with Execution
    with Loggable
    with JsonConverterProvider =>

  import jsonConverter.MapJsonFormat

  protected val eventSubscriberWatcherActor = system.actorOf(
    Props(classOf[ServerNotifications.EventSubscriberWatcher], this))

  protected val serverEventsSource =
    Source.actorRef[Any](PartialFunction.empty, PartialFunction.empty, 16, OverflowStrategy.dropNew)

  protected val wsNotificationGraph = {
    Flow.fromSinkAndSourceCoupledMat(
      Sink.ignore, // ignore incoming messages from client
      serverEventsSource
    ) ((_, actor) => actor)
      .map(m => TextMessage.Strict(createServerEvent(m).data))
        .withAttributes(ActorAttributes.supervisionStrategy{
        case ex: Exception =>
          logger.error("WsNotificationGraph crashed", ex)
          Supervision.Stop
      })
    }

    protected def createServerEvent(event: Any): ServerSentEvent = event match {
      case ctx: DeferredContext => notifyDeferredStatus(ctx)
      case x => notifyUserEvent(x)
    }
    /* ***********************
    *** Event notification ***
    **************************/
    def serverSideEventAction(userIdString: String): Route = Directives.complete {
      serverEventsSource
        .map(createServerEvent)
        .mapMaterializedValue(eventSubscriberWatcherActor ! ServerNotifications.EventSubscriberActorMsg(_, userIdString))
    }
    /**
      * @deprecated use {{{serverSideEventAction}}}
      * */
    @deprecated("Use serverSideEventAction instead", "6.0.0")
    def wsNotificationsAction(userIdString: String) = {
      handleWebSocketMessages(wsNotificationGraph.mapMaterializedValue(
        eventSubscriberWatcherActor ! ServerNotifications.EventSubscriberActorMsg(_, userIdString)))
    }
    private def notifyDeferredStatus(ctx: DeferredContext): ServerSentEvent =
      new ServerSentEvent(Map(ctx.hash -> Map(
        "status" -> ctx.status,
        "time" -> Option(ctx.responseTime).getOrElse(ctx.requestTime)))
        .asInstanceOf[Map[String, Any]]
        .toJson
        .compactPrint
      )
    private def notifyUserEvent(event: Any): ServerSentEvent = new ServerSentEvent(event match {
      case m: Map[String, Any]@unchecked => m.toJson.compactPrint
      case j: JsValue => j.compactPrint
      case x => String valueOf x
    })
    def publishUserEvents(user: String, events: Iterable[Any]) = {
      events.foreach(publishUserEvent(user, _))
    }
    def publishUserEvent(user: String, event: Any) = {
      import ServerNotifications._
      val addressee = UserAddresseeMsg(user)
      EventBus.publish(EventBus.Message(addressee, event))
    }
    /** Return all actual user events client through web socket should be notified about.
    Is called when web socket connection is established. Must be overrided by subclasses. */
    def getActualUserEvents(user: String): Iterable[Any] = Nil
    /* End of event notification */
}

object ServerNotifications extends Loggable {

  /** Publishes events to newly created websocket */
  trait InitialEventsPublisher {
    def publishInitialEvents(userIdString: String): Unit
  }

  trait NoInitialEvents extends InitialEventsPublisher {
    def publishInitialEvents(user: String): Unit = {}
  }

  /** Publishes app version and deferred events status info */
  trait DefaultInitialEventsPublisher extends InitialEventsPublisher {
      this: ServerNotifications with AppVersion with DeferredStatusPublisher =>
    def publishInitialEvents(user: String): Unit = {
      publishUserEvent(user, Map("version" -> appVersion).toJson.compactPrint)
      publishUserDeferredStatuses(user)
      publishUserEvents(user, getActualUserEvents(user))
    }
  }

  trait Addressee
  case class UserAddresseeMsg(user: String) extends Addressee
  case class EventSubscriberActorMsg(actor: ActorRef, user: String)

  class EventSubscriberWatcher(publisher: InitialEventsPublisher) extends Actor with akka.actor.ActorLogging {
    override def preStart() = {
      logger.info(s"EventSubscriberWatcher actor started")
    }
    override def receive = {
      case EventSubscriberActorMsg(actor, user: String) =>
        context watch actor
        EventBus.subscribe(actor, UserAddresseeMsg(user))
        publisher.publishInitialEvents(user)
      case Terminated(actor) =>
        EventBus.unsubscribe(actor)
        context unwatch actor
    }
    override def postStop() = {
      logger.info(s"EventSubscriberWatcher actor stopped")
    }
  }

  case class MsgEnvelope(topic: String, payload: Any)
  case class DeferredNotification(value: JsValue)

  def publish(msgEnvelope: MsgEnvelope)(implicit serverNotif: ServerNotifications): Unit = {
    serverNotif.publishUserEvent(msgEnvelope.topic, msgEnvelope.payload match {
      case DeferredNotification(value) => value.compactPrint
      case x => x
    })
  }
}
