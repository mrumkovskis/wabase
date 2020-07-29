package org.wabase

import akka.NotUsed
import akka.stream.scaladsl.Source
import org.tresql._
import akka.stream.{Attributes, Outlet, SourceShape}
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}


class QuerySource(tresql: String, params: Map[String, Any], resources: Resources)
extends GraphStage[SourceShape[Seq[Any]]] {
  val out = Outlet[Seq[Any]]("row")
  val shape = SourceShape(out)
  override def createLogic(attrs: Attributes) = new GraphStageLogic(shape) {
    var result: Result[RowLike] = _
    override def preStart = { result = Query(tresql, params)(resources) }
    override def postStop = {
      result.close
      resources.conn.close
    }
    setHandler(out, new OutHandler {
      override def onPull = if (result.hasNext) {
        result.next
        push(out, result.values)
      } else completeStage
    })
  }
}


object ResultSource {
  def apply[A <: RowLike](result: => Result[A]): Source[A, NotUsed] =
    Source.fromGraph(new ResultSource(result))
}

class ResultSource[A <: RowLike](result: => Result[A]) extends GraphStage[SourceShape[A]] {
  val out = Outlet[A]("row")
  val shape = SourceShape(out)
  override def createLogic(inheritedAttributes: Attributes) = new GraphStageLogic(shape) {
    var res: Result[A] = _
    override def preStart(): Unit = res = result
    override def postStop(): Unit = res.closeWithDb
    setHandler(out, new OutHandler {
      override def onPull(): Unit = if (res.hasNext) {
        push(out, res.next())
      } else completeStage()
    })
  }
}
