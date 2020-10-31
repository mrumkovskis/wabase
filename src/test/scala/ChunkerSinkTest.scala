package org.wabase

import akka.stream.scaladsl.Source
import akka.util.ByteString
import org.scalatest.flatspec.AsyncFlatSpec

import scala.concurrent.Future

class ChunkerSinkTest extends AsyncFlatSpec {
  import scala.language.postfixOps

  import StreamsEnv._

  private def src(n: Int) =
    Source.fromIterator(() => 0 to n iterator).map(b => ByteString(b.toByte))

  private def res(n: Int) =
    0 to n map(b => ByteString(b.toByte)) reduce(_ ++ _)

  private def incompleteTest(n: Int) =
    validateIncomplete(List(src(n).runWith(new ChunkerSink).map(_ -> res(n))))

  private def validateIncomplete(res: List[Future[(SourceValue, ByteString)]]) =
    Future.foldLeft {
      res.map(_.flatMap {
        case (IncompleteSourceValue(s), x) => s.runReduce(_ ++ _).map(_ -> x)
        case (CompleteSourceValue(s), x) => Future.successful(s -> x)
      })
    } (List[(Any, ByteString)]()) { (l, r) => r :: l }
    .map(_.unzip)
    .map { case (a, b) => assert (a == b) }

  it should "return CompleteSourceValue" in {
    val n = 0
    src(n).runWith(new ChunkerSink)
      .map {
        case CompleteSourceValue(v) => assert(v == res(n))
        case IncompleteSourceValue(s) => assert(false)
      }
  }
  it should "return InCompleteSourceValue of 2 bytes" in {
    incompleteTest(1)
  }
  it should "return InCompleteSourceValue of 3 bytes" in {
    incompleteTest(2)
  }
  it should "return CompleteSourceValue from RowSource" in {
    val n = 4
    RowSource.value(n + 2, 0, src(n)).map {
      case CompleteSourceValue(v) => assert(v == res(n))
      case IncompleteSourceValue(s) => assert(false)
    }
  }
  it should "return IncompleteSourceValue(s) from RowSource" in {
    val (a, b) = (5, 100)
    val r = (a to b map (n => RowSource.value(a, b, src(n)).map(_ -> res(n)))).toList
    validateIncomplete(r)
  }
}
