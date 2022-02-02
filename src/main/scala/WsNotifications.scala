package org.wabase

import akka.http.scaladsl.server.directives.WebSocketDirectives
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.stream.{OverflowStrategy, ActorAttributes, Supervision}
import akka.stream.scaladsl.{Source, Flow, Sink }

import akka.actor.{Actor, Props, ActorRef, Terminated, PoisonPill}

import spray.json._
import DefaultJsonProtocol._
import DeferredControl._

trait WsNotifications extends WebSocketDirectives {
  this: WsInitialEventsPublisher
    with Execution
    with Loggable
    with JsonConverterProvider =>

  import jsonConverter.MapJsonFormat

  protected val wsSubscriberWatcherActor = system.actorOf(
    Props(classOf[WsNotifications.WsSubscriberWatcher], this))

  val wsNotificationGraph = {
    Flow.fromSinkAndSourceCoupledMat(
      Sink.ignore, // ignore incoming messages from client
      Source.actorRef[Any](PartialFunction.empty, PartialFunction.empty, 16, OverflowStrategy.dropNew)) {
        (_, actor) => actor
      }.map {
        case ctx: DeferredContext => notifyDeferredStatus(ctx)
        case x => notifyUserEvent(x)
      }.withAttributes(ActorAttributes.supervisionStrategy{
        case ex: Exception =>
          logger.error("WsNotificationGraph crashed", ex)
          Supervision.Stop
      })
    }
    /* ***********************
    *** Event notification ***
    **************************/
    def wsNotificationsAction(userIdString: String) = {
      handleWebSocketMessages(wsNotificationGraph.mapMaterializedValue(
        wsSubscriberWatcherActor ! WsNotifications.WsActorRegister(_, userIdString)))
    }
    private def notifyDeferredStatus(ctx: DeferredContext): Message =
      TextMessage(Map(ctx.hash -> Map(
        "status" -> ctx.status,
        "time" -> Option(ctx.responseTime).getOrElse(ctx.requestTime)))
        .asInstanceOf[Map[String, Any]]
        .toJson
        .prettyPrint
      )
    private def notifyUserEvent(event: Any): Message = event match {
      case m: Map[String, Any]@unchecked => TextMessage(m.toJson.prettyPrint)
      case j: JsValue => TextMessage(j.prettyPrint)
      case x => TextMessage(String valueOf x)
    }
    def publishUserEvents(user: String, events: Iterable[Any]) = {
      events.foreach(publishUserEvent(user, _))
    }
    def publishUserEvent(user: String, event: Any) = {
      import WsNotifications._
      val addressee = UserAddressee(user)
      EventBus.publish(EventBus.Message(addressee, event))
    }
    /** Return all actual user events client through web socket should be notified about.
    Is called when web socket connection is established. Must be overrided by subclasses. */
    def getActualUserEvents(user: String): Iterable[Any] = Nil
    /* End of event notification */
}

object WsNotifications extends Loggable {

  /** Publishes events to newly created websocket */
  trait WsInitialEventsPublisher {
    def publishInitialWsEvents(userIdString: String): Unit
  }

  trait NoWsInitialEvents extends WsInitialEventsPublisher {
    def publishInitialWsEvents(user: String): Unit = {}
  }

  /** Publishes app version and deferred events status info */
  trait DefaultWsInitialEventsPublisher extends WsInitialEventsPublisher {
      this: WsNotifications with AppVersion with DeferredStatusPublisher =>
    def publishInitialWsEvents(user: String): Unit = {
      publishUserEvent(user, Map("version" -> appVersion).toJson.prettyPrint)
      publishUserDeferredStatuses(user)
      publishUserEvents(user, getActualUserEvents(user))
    }
  }

  trait Addressee
  case class UserAddressee(user: String) extends Addressee
  case class WsActorRegister(actor: ActorRef, user: String)

  class WsSubscriberWatcher(publisher: WsInitialEventsPublisher) extends Actor with akka.actor.ActorLogging {
    override def preStart() = {
      logger.info(s"WsSubscriberWatcher actor started")
    }
    override def receive = {
      case WsActorRegister(actor, user: String) =>
        context watch actor
        EventBus.subscribe(actor, UserAddressee(user))
        publisher.publishInitialWsEvents(user)
      case Terminated(actor) =>
        EventBus.unsubscribe(actor)
        context unwatch actor
    }
    override def postStop() = {
      logger.info(s"WsSubscriberWatcher actor stopped")
    }
  }

  case class MsgEnvelope(topic: String, payload: Any)
  case class DeferredNotification(value: JsValue)

  def publish(msgEnvelope: MsgEnvelope)(implicit ws: WsNotifications): Unit = {
    ws.publishUserEvent(msgEnvelope.topic, msgEnvelope.payload match {
      case DeferredNotification(value) => value.prettyPrint
      case x => x
    })
  }
}
