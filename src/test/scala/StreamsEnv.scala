package org.wabase

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer

/** Materializer extracted into separate object due to scalatest bug - did not launch test suite. */
object StreamsEnv {
  implicit val mat: Materializer = Materializer(ActorSystem("wabase-test"))
}
