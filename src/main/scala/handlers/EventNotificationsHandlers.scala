package org.wabase.handlers

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.HttpRequest
import org.wabase.ServerNotifications

trait EventNotificationsHandlers {
  def subscribeToEvent(topic: String)(as: ActorSystem, req: HttpRequest) = {
    ServerNotifications.subscribeToEventsAndListen(b => a => b.subscribe(a, topic), _ => ())(as, req)
  }
  def subscribeToWsMessages(topic: String)(as: ActorSystem, req: HttpRequest) = {
    ServerNotifications.subscribeToWsMessagesAndListen(b => a => b.subscribe(a, topic), _ => ())(as, req)
  }
}

object EventNotificationsHandlers extends EventNotificationsHandlers
