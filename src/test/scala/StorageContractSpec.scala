package org.wabase

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.util.ByteString
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers
import org.wabase.fs.Storage

import java.io.File
import java.nio.file.Files
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

/** Tests every Storage implementation must pass. Storage is expected to be configured with path-format = days. */
abstract class StorageContractSpec(storageName: String) extends FlatSpec with Matchers with BeforeAndAfterAll {
  behavior of storageName

  implicit val system: ActorSystem = ActorSystem(getClass.getSimpleName)
  protected implicit val ec: ExecutionContext = system.dispatcher

  /** New empty storage with path-format = days */
  def newStorage(): Storage
  /** Whether verify detects wrong content of correct size (storage provides content checksum) */
  def verifiesContent: Boolean = true

  override def afterAll(): Unit = {
    Await.result(org.apache.pekko.http.scaladsl.Http().shutdownAllConnectionPools(), maxWait)
    Await.result(system.terminate(), maxWait)
    super.afterAll()
  }

  protected val maxWait = 10.seconds
  protected def await[T](f: Future[T]): T = Await.result(f, maxWait)

  protected val content = ByteString("Storage contract specs content")
  protected val contentSha =
    java.security.MessageDigest.getInstance("SHA-256").digest(content.toArray).map("%02x".format(_)).mkString
  protected lazy val localTmpDir = Files.createTempDirectory("storage-contract-specs").toFile
  protected def tmpFile(data: ByteString = content): File = {
    val f = File.createTempFile("tmp", null, localTmpDir)
    Files.write(f.toPath, data.toArray)
    f
  }
  protected def read(storage: Storage, fullName: String): ByteString =
    await(storage.sourceFor(fullName).runFold(ByteString.empty)(_ ++ _))

  it should "store file, consume source file and read it back" in {
    val storage = newStorage()
    val src = tmpFile()
    val tailPath = await(storage.storeFile(src, contentSha))
    tailPath should fullyMatch regex """\d{4}/\d{2}/\d{2}"""
    src.exists shouldBe false
    read(storage, s"$tailPath/$contentSha") shouldBe content
  }

  it should "store file with the same name again" in {
    val storage = newStorage()
    val tailPath = await(storage.storeFile(tmpFile(), contentSha))
    val src = tmpFile()
    await(storage.storeFile(src, contentSha)) shouldBe tailPath
    src.exists shouldBe false
    read(storage, s"$tailPath/$contentSha") shouldBe content
  }

  it should "verify stored file" in {
    val storage = newStorage()
    val tailPath = await(storage.storeFile(tmpFile(), contentSha))
    val name = s"$tailPath/$contentSha"
    await(storage.verify(name, content.length, contentSha)) shouldBe Some(true)
    await(storage.verify(name, content.length + 1, contentSha)) shouldBe Some(false)
    if (verifiesContent)
      await(storage.verify(name, content.length, "0" * 64)) shouldBe Some(false)
    await(storage.verify(s"$tailPath/${"1" * 64}", content.length, contentSha)) shouldBe None
  }

  it should "copy stored file" in {
    val storage = newStorage()
    val tailPath = await(storage.storeFile(tmpFile(), contentSha))
    val name = s"$tailPath/$contentSha"
    await(storage.copy(name, s"copies/nested/$contentSha"))
    read(storage, s"copies/nested/$contentSha") shouldBe content
    read(storage, name) shouldBe content
    await(storage.copy(name, name)) // same file - no-op
    read(storage, name) shouldBe content
  }

  it should "fail to read missing file" in {
    val storage = newStorage()
    an [Exception] should be thrownBy read(storage, s"2000/01/01/${"2" * 64}")
  }
}
