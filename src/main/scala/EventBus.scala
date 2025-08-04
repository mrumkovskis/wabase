package org.wabase


import org.apache.pekko.event.{ActorEventBus, LookupClassification}

case class EventMessage(topic: Any, payload: Any)

trait EventBus extends ActorEventBus with LookupClassification {

  override type Event = EventMessage
  override type Classifier = Any

  override protected def publish(event: Event, subscriber: Subscriber) = subscriber ! event.payload
  override protected def classify(event: Event): Classifier = event.topic

  override protected def mapSize(): Int = 128
}

object EventBus extends EventBus
