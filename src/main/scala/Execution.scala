package org.wabase

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.ActorMaterializer
import org.apache.pekko.stream.Materializer
import scala.concurrent.ExecutionContextExecutor

trait Execution {
  protected def execution: Execution
  implicit val system: ActorSystem = execution.system
  implicit lazy val executor: ExecutionContextExecutor = execution.executor
}

class ExecutionImpl(
  override implicit val system: ActorSystem) extends Execution {
  override protected def execution: Execution = this
  override lazy val executor: ExecutionContextExecutor = system.dispatcher
}
