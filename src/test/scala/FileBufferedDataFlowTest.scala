package org.wabase

import java.io.File

import akka.util.ByteString
import org.scalatest.flatspec.AsyncFlatSpec
import akka.stream.scaladsl._

import scala.util.Random

class FileBufferedDataFlowTest extends AsyncFlatSpec {
  import scala.language.postfixOps
  def testBufferedFlow(n: Int, bufferSize: Int, maxFileSize: Long, outBufSize: Int = 1024 * 8) = {
    val buffer = FileBufferedFlow.create(bufferSize, maxFileSize, outBufSize)
    Source.fromIterator(() => 0 to n iterator).map{ b => ByteString(b.toByte) }.async.via(buffer)
  }

  import StreamsEnv._

  val size = 100
  val pattern = ByteString(0 to size map (_.toByte) toArray)
  val source = testBufferedFlow(size, 10, 100, 10)
  it should "buffer bytes flow with fixed downstream timeout" in {
    source.map {x => Thread.sleep(10); x} runReduce { _ ++ _ } map { b => assert(pattern == b) }
  }
  it should "buffer bytes flow with no downstream timeout" in {
    source.runReduce { _ ++ _ } map { b => assert(pattern == b) }
  }
  it should "buffer bytes flow with variable downstream timeout" in {
    source.map {x => Thread.sleep(Random.nextInt(101)); x} runReduce { _ ++ _ } map { b => assert(pattern == b) }
  }
  it should "buffer bytes from file with variable downstream timeout" in {
    val fileSize = 1000000
    val bufferSize = 1024 * 4
    val maxFileSize = 1024 * 1024
    val outBufSize = 1024 * 4
    val source = Source.fromIterator(() => 0 to fileSize iterator).map { b => ByteString(b.toByte) }
    val buffer = FileBufferedFlow.create(bufferSize, maxFileSize, outBufSize)
    val file = File.createTempFile("buffer_test_file", ".data")
    source.runWith(FileIO.toPath(file.toPath))
      .flatMap(_ => FileIO.fromPath(file.toPath).async
        .via(buffer)
        .map { b => Thread.sleep(Random.nextInt(11)); b }
        .runWith(AppFileStreamer.sha256sink))
      .zip(source.runWith(AppFileStreamer.sha256sink))
      .map { case (hash1, hash2) => assert(hash1 == hash2) }
      .andThen { case _ => file.delete }
  }
}
