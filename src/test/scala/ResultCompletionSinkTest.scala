package org.wabase

import akka.NotUsed
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.util.ByteString
import org.scalatest.flatspec.AsyncFlatSpec

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

class ResultCompletionSinkTest extends AsyncFlatSpec {
  import scala.language.postfixOps

  import StreamsEnv._

  private def src(n: Int) =
    Source.fromIterator(() => 0 to n iterator).map(b => ByteString(b.toByte))

  private def res(n: Int) =
    0 to n map(b => ByteString(b.toByte)) reduce(_ ++ _)

  private def incompleteTest(n: Int) =
    validateIncomplete(List(src(n).runWith(new ResultCompletionSink).map(_.head -> res(n))))

  private def incompleteMultipleTest(n: Int, c: Int) = {
    val r = res(n)
    src(n).runWith(new ResultCompletionSink(resultCount = c)).map(_.map(_ -> r))
      .flatMap { s =>
        Future.sequence(s.map(t => Future(validateIncomplete(List(Future.successful(t))))))
      }
      .flatMap { s =>
        Future.sequence(s)
      }
      .map (_ => assert(true))
  }

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
    src(n).runWith(new ResultCompletionSink).map(_.head)
      .map {
        case CompleteResult(v) => assert(v == res(n))
        case IncompleteResultSource(_) => assert(false)
      }
  }
  it should "return empty CompleteResult on empty source" in {
    Source.empty[ByteString].runWith(new ResultCompletionSink).map(_.head)
      .map {
        case CompleteResult(v) => assert(v == ByteString.empty)
        case IncompleteResultSource(_) => assert(false)
      }
  }
  it should "return IncompleteResultSource of 2 bytes" in {
    incompleteTest(1)
  }
  it should "return IncompleteResultSource of 3 bytes" in {
    incompleteTest(2)
  }

  it should "return multiple IncompleteResult" in {
    incompleteMultipleTest(10, 3)
  }
  it should "complete sink if any or materialized source fail" in {
    val n = 100
    src(n).runWith(new ResultCompletionSink(2)).mapTo[Seq[IncompleteResultSource[NotUsed]]].flatMap { r =>
      Future {
        r.head.result.runFold(0) { (x, _) =>
          if (x == 20) sys.error("err")
          Thread.sleep(10)
          x + 1
        }
      }
      validateIncomplete(r.tail.map(r => Future.successful(r -> res(n))).toList)
    }
  }
  it should "complete sink if all materialized sources fail" in {
    val n = 100
    val (ignoreF, dataF) =
      src(n).alsoToMat(Sink.ignore)(Keep.right).toMat(new ResultCompletionSink())(Keep.both).run()

    dataF.mapTo[Seq[IncompleteResultSource[NotUsed]]].flatMap { r =>
      Future {
        r.head.result.runFold(0) { (x, _) =>
          if (x == 20) {
            sys.error("err")
          }
          Thread.sleep(10)
          x + 1
        }
      }
    }
    Future {
      Await.result(ignoreF, 5.seconds)
    }(scala.concurrent.ExecutionContext.global) // somehow must use other execution context to perform future above concurrently ???
    .map(_ => assert(true))
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
