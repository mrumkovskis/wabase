package scala.jdk

import java.util.concurrent.CompletionStage
import scala.concurrent.Future
import scala.concurrent.java8.FuturesConvertersImpl._

object FutureConverters {
  def toScala[T](cs: CompletionStage[T]): Future[T] = {
    cs match {
      case cf: CF[T] => cf.wrapped
      case _ =>
        val p = new P[T](cs)
        cs whenComplete p
        p.future
    }
  }
  implicit def CompletionStageOps[T](cs: CompletionStage[T]): CompletionStageOps[T] = new CompletionStageOps(cs)

  final class CompletionStageOps[T](val __self: CompletionStage[T]) extends AnyVal {
    def asScala: Future[T] = FutureConverters.toScala(__self)
  }
}
