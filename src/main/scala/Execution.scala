package org.wabase

import org.apache.pekko.actor.ActorSystem

import scala.concurrent.ExecutionContext

trait Execution {
  implicit def actorSystem: ActorSystem
  implicit lazy val executionContext: ExecutionContext = actorSystem.dispatcher
}
