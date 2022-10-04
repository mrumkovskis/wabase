package org.wabase

import java.io.File

import akka.util.ByteString
import org.scalatest.flatspec.AsyncFlatSpec
import akka.stream.scaladsl._

import scala.util.Random

class FileBufferedDataFlowTest extends AsyncFlatSpec {
  import scala.language.postfixOps
  def testBufferedFlow(n: Int, bufferSize: Int, maxFileSize: Long, outBufSize: Int = 1024 * 8) = {
    val buffer = FileBufferedFlow.create(bufferSize, maxFileSize, outBufSize)()
    Source.fromIterator(() => 0 to n iterator).map{ b => ByteString(b.toByte) }.via(buffer).async
  }

  import StreamsEnv._

  val size = 100
  val pattern = ByteString(0 to size map (_.toByte) toArray)
  val source = testBufferedFlow(size, 10, 100, 10)
  it should "buffer bytes flow with fixed downstream timeout" in {
    source.map {x => Thread.sleep(10); x}.async runReduce { _ ++ _ } map { b => assert(pattern == b) }
  }
  it should "buffer bytes flow with no downstream timeout" in {
    source.runReduce { _ ++ _ } map { b => assert(pattern == b) }
  }
  it should "buffer bytes flow with variable downstream timeout" in {
    source.map {x => Thread.sleep(Random.nextInt(101)); x}.async runReduce { _ ++ _ } map { b => assert(pattern == b) }
  }
  it should "buffer bytes from flow with variable upstream/downstream timeout" in {
    val fileSize = 1000000
    val bufferSize = 1024
    val maxFileSize = 1024 * 1024
    val outBufSize = 1024 * 2
    val source = Source.fromIterator(() => 0 to fileSize iterator).map { b => ByteString(b.toByte) }
    val buffer = FileBufferedFlow.create(bufferSize, maxFileSize, outBufSize)()
    @volatile var size = 0d
    source
      .aggregateWithBoundary(() => ByteString.empty)((abs, bs) => (abs ++ bs, abs.size + bs.size > 512), identity, None)
      .map { b => size += b.size; b }
      .map { b =>
        val r = size / fileSize
        if (r < 0.25 || (r > 0.50 && r < 0.75)) {
          Thread.sleep(Random.nextInt(30))
        }
        b
      }.async
      .via(buffer).async
      .map { b =>
        val r = size / fileSize
        if ((r > 0.25 && r < 0.5) || r > 0.75) {
          Thread.sleep(Random.nextInt(30))
        }
        b
      }
      .runWith(AppFileStreamer.sha256sink)
      .zip(source.runWith(AppFileStreamer.sha256sink))
      .map { case (hash1, hash2) => assert(hash1 == hash2) }
  }
}
