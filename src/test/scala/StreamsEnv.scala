package org.wabase

import akka.actor.ActorSystem
import akka.stream.Materializer

/** Materializer extracted into separate object due to scalatest bug - did not launch test suite. */
object StreamsEnv {
  implicit val mat: Materializer = Materializer(ActorSystem("wabase-test"))
}
