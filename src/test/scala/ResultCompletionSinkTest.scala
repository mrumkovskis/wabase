package org.wabase

import akka.stream.scaladsl.Source
import akka.util.ByteString
import org.scalatest.flatspec.AsyncFlatSpec

import scala.concurrent.Future

class ResultCompletionSinkTest extends AsyncFlatSpec {
  import scala.language.postfixOps

  import StreamsEnv._

  private def src(n: Int) =
    Source.fromIterator(() => 0 to n iterator).map(b => ByteString(b.toByte))

  private def res(n: Int) =
    0 to n map(b => ByteString(b.toByte)) reduce(_ ++ _)

  private def incompleteTest(n: Int) =
    validateIncomplete(List(src(n).runWith(new ResultCompletionSink).map(_ -> res(n))))

  private def validateIncomplete(res: List[Future[(SerializedResult, ByteString)]]) =
    Future.foldLeft {
      res.map(_.flatMap {
        case (IncompleteResultSource(s), x) => s.runReduce(_ ++ _).map(_ -> x)
        case (CompleteResult(s), x) => Future.successful(s -> x)
      })
    } (List[(Any, ByteString)]()) { (l, r) => r :: l }
    .map(_.unzip)
    .map { case (a, b) => assert (a == b) }

  it should "return CompleteResult" in {
    val n = 0
    src(n).runWith(new ResultCompletionSink)
      .map {
        case CompleteResult(v) => assert(v == res(n))
        case IncompleteResultSource(s) => assert(false)
      }
  }
  it should "return empty CompleteResult on empty source" in {
    Source.empty[ByteString].runWith(new ResultCompletionSink)
      .map {
        case CompleteResult(v) => assert(v == ByteString())
        case IncompleteResultSource(_) => assert(false)
      }
  }
  it should "return IncompleteResultSource of 2 bytes" in {
    incompleteTest(1)
  }
  it should "return IncompleteResultSource of 3 bytes" in {
    incompleteTest(2)
  }
  it should "return CompleteResult from RowSource" in {
    val n = 4
    RowSource.value(n + 2, 0, src(n)).map {
      case CompleteResult(v) => assert(v == res(n))
      case IncompleteResultSource(s) => assert(false)
    }
  }
  it should "return IncompleteResultSource(s) from RowSource" in {
    val (a, b) = (5, 100)
    val r = (a to b map (n => RowSource.value(a, b, src(n)).map(_ -> res(n)))).toList
    validateIncomplete(r)
  }
}
