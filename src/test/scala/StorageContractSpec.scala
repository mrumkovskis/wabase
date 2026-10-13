package org.wabase

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.util.ByteString
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers
import org.wabase.fs.Storage

import java.io.File
import java.nio.file.Files
import java.time.{Duration => JDuration}
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
  protected def listAged(storage: Storage, minAge: JDuration): Seq[String] =
    await(storage.listAged(minAge).runFold(Vector.empty[String])(_ :+ _))
  /** Lets just stored files become older than zero min-age */
  protected def ageFiles(): Unit = Thread.sleep(20)
  protected val Trash = "trash"
  protected val anHour = JDuration.ofHours(1)

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

  it should "have location without trailing slash" in {
    val storage = newStorage()
    storage.location should not endWith "/"
    storage.location should not equal newStorage().location
  }

  it should "list aged stored files matching stored file layout only" in {
    val storage = newStorage()
    val tailPath = await(storage.storeFile(tmpFile(), contentSha))
    val name = s"$tailPath/$contentSha"
    val stored = Seq(
      name,
      s"2000/01/01/${"3" * 64}",
      s"2000/01/01/02/${"3" * 64}",
      s"2000/01/01/02/03/${"3" * 64}",
    )
    val notStored = Seq(
      "other/file.txt",
      s"2000/01/01/$contentSha.txt",
      s"2000/01/01/${"g" * 64}",
      s"2000/01/$contentSha",
      s"2000/01/01/02/03/04/$contentSha",
      s"20000/01/01/$contentSha",
      s"$Trash/2000/01/01/$contentSha",
      s"tmp/2000/01/01/$contentSha",
      s"nested/2000/01/01/$contentSha",
    )
    (stored.tail ++ notStored).foreach(n => await(storage.copy(name, n)))
    listAged(storage, anHour) shouldBe empty
    ageFiles()
    listAged(storage, JDuration.ZERO) should contain theSameElementsAs stored
  }

  it should "move aged file to trash and clean trash" in {
    val storage = newStorage()
    val tailPath = await(storage.storeFile(tmpFile(), contentSha))
    val name = s"$tailPath/$contentSha"
    await(storage.cleanTrash()) shouldBe 0
    await(storage.trashAged(name, anHour)) shouldBe Some(false)
    read(storage, name) shouldBe content
    ageFiles()
    await(storage.trashAged(name, JDuration.ZERO)) shouldBe Some(true)
    an [Exception] should be thrownBy read(storage, name)
    read(storage, s"$Trash/$name") shouldBe content
    listAged(storage, JDuration.ZERO) shouldBe empty
    await(storage.trashAged(name, JDuration.ZERO)) shouldBe None
    await(storage.cleanTrash()) shouldBe 1
    an [Exception] should be thrownBy read(storage, s"$Trash/$name")
    await(storage.cleanTrash()) shouldBe 0
  }

  it should "refuse to trash file not matching stored file layout" in {
    val storage = newStorage()
    val tailPath = await(storage.storeFile(tmpFile(), contentSha))
    val name = s"$tailPath/$contentSha"
    val other = s"other/$contentSha"
    await(storage.copy(name, other))
    ageFiles()
    an [IllegalArgumentException] should be thrownBy await(storage.trashAged(other, JDuration.ZERO))
    read(storage, other) shouldBe content
  }
}
