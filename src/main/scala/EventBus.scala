package org.wabase


import akka.event.{ActorEventBus, LookupClassification}

trait EventBus extends ActorEventBus with LookupClassification {

  case class Message(topic: Any, payload: Any)

  override type Event = Message
  override type Classifier = Any

  override protected def publish(event: Event, subscriber: Subscriber) = subscriber ! event.payload
  override protected def classify(event: Event): Classifier = event.topic

  override protected def mapSize(): Int = 128
}

object EventBus extends EventBus
