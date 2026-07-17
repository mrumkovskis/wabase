package org.wabase

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.ActorMaterializer
import org.apache.pekko.stream.Materializer
import scala.concurrent.ExecutionContextExecutor

trait Execution {
  /** Delegate used by wrappers that do not own the actor system. */
  protected def execution: Execution
  implicit def system: ActorSystem
  implicit def executor: ExecutionContextExecutor
}

class ExecutionImpl(
  override implicit val system: ActorSystem) extends Execution {
  override protected def execution: Execution = this
  override lazy val executor: ExecutionContextExecutor = system.dispatcher
}
