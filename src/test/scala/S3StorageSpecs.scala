package org.wabase

import com.typesafe.config.ConfigException
import org.apache.pekko.util.ByteString
import org.wabase.fs.{S3Storage, Storage}

import java.nio.file.Files
import java.util.UUID

class S3StorageSpecs extends StorageContractSpec("S3Storage") {
  private val bucket = "s3-storage-specs"
  private val server = new S3TestServer().start()
  server.createBucket(bucket)

  override def afterAll(): Unit = {
    server.stop()
    super.afterAll()
  }

  private def localTmpPath = Files.createTempDirectory("s3-storage-specs-tmp").toFile.getAbsolutePath
  private def s3Storage(root: String, extra: String = "") =
    new S3Storage(server.filesConfig(root, localTmpPath, extra))

  override def newStorage(): Storage = s3Storage(s"$bucket/${UUID.randomUUID()}/nested-prefix")
  /** S3Proxy does not return checksum, S3Storage verifies size only then. Real S3 returns checksum. */
  override def verifiesContent: Boolean = false

  it should "store file in bucket without prefix" in {
    val otherBucket = "s3-storage-specs-no-prefix"
    server.createBucket(otherBucket)
    val storage = s3Storage(otherBucket)
    val tailPath = await(storage.storeFile(tmpFile(), contentSha))
    read(storage, s"$tailPath/$contentSha") shouldBe content
  }

  it should "list and trash aged files in bucket without prefix" in {
    val otherBucket = "s3-storage-specs-no-prefix-trash"
    server.createBucket(otherBucket)
    val storage = s3Storage(otherBucket)
    storage.location shouldBe s"s3://$otherBucket"
    val tailPath = await(storage.storeFile(tmpFile(), contentSha))
    val name = s"$tailPath/$contentSha"
    ageFiles()
    await(storage.trashAged(name, java.time.Duration.ZERO)) shouldBe Some(true)
    await(storage.storeFile(tmpFile(), contentSha))
    ageFiles()
    listAged(storage, java.time.Duration.ZERO) shouldBe Seq(name)
    await(storage.cleanTrash()) shouldBe 1
    read(storage, name) shouldBe content
  }

  it should "store large file using multipart upload" in {
    val storage = s3Storage(s"$bucket/${UUID.randomUUID()}", "files.s3-multipart-threshold = 10")
    val data = ByteString("multipart " * 100)
    val sha = java.security.MessageDigest.getInstance("SHA-256").digest(data.toArray).map("%02x".format(_)).mkString
    val tailPath = await(storage.storeFile(tmpFile(data), sha))
    read(storage, s"$tailPath/$sha") shouldBe data
    await(storage.verify(s"$tailPath/$sha", data.length, sha)) shouldBe Some(true)
  }

  it should "store file without checksum header" in {
    val storage = s3Storage(s"$bucket/${UUID.randomUUID()}", "files.s3-checksum = false")
    val tailPath = await(storage.storeFile(tmpFile(), contentSha))
    await(storage.verify(s"$tailPath/$contentSha", content.length, contentSha)) shouldBe Some(true)
  }

  it should "require tmp-path" in {
    val cfg = server.filesConfig(bucket, localTmpPath).withoutPath("files.tmp-path")
    val ex = intercept[RuntimeException] {
      @annotation.nowarn("msg=Manifest")
      val storage = getObjectOrNewInstance[Storage](cfg, "files.storage-class", "file streamer file storage", Seq(cfg))
      storage
    }
    ex.getCause shouldBe a [ConfigException.Missing]
    ex.getMessage should include ("files.tmp-path")
  }

  it should "report upload failure with location" in {
    val storage = s3Storage("missing-bucket/prefix")
    val ex = intercept[RuntimeException](await(storage.storeFile(tmpFile(), contentSha)))
    ex.getMessage should include (s"s3://missing-bucket/prefix/")
  }
}
