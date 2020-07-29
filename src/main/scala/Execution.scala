package org.wabase

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.Materializer
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
