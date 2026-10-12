package org.wabase

import com.typesafe.config.{ConfigException, ConfigFactory}
import org.wabase.fs.Storage

import java.io.File
import java.nio.file.Files

class FsStorageSpecs extends StorageContractSpec("FsStorage") {
  override def newStorage(): Storage = fsStorage(newRoot)

  it should "report invalid path format" in {
    val cfg = ConfigFactory.parseString(
      """files.root = fs-storage-specs
        |files.path-format = weeks
        |files.storage-class = org.wabase.fs.FsStorage""".stripMargin)
    val ex = intercept[RuntimeException] {
      getObjectOrNewInstance[org.wabase.fs.Storage](cfg, "files.storage-class", "file streamer file storage", Seq(cfg))
    }
    ex.getMessage should include ("Invalid path format 'weeks', must be one of: days, hours, minutes")
    ex.getCause shouldBe a [ConfigException.BadValue]
  }

  private def fsStorage(root: String) = {
    val cfg = ConfigFactory.parseString(
      s"""files.root = "$root"
         |files.path-format = days""".stripMargin)
    new org.wabase.fs.FsStorage(cfg)
  }
  private def newRoot = Files.createTempDirectory("fs-storage-specs").toFile.getAbsolutePath

  it should "refresh modification time of already stored file if move fails" in {
    val root = newRoot
    val name = "a" * 64
    val tailPath = new java.text.SimpleDateFormat("yyyy/MM/dd").format(new java.util.Date)
    val target = new File(s"$root/$tailPath/$name")
    target.getParentFile.mkdirs
    Files.write(target.toPath, "content".getBytes("UTF-8"))
    val oldTime = System.currentTimeMillis - 2 * 24 * 3600 * 1000L
    target.setLastModified(oldTime) shouldBe true

    // missing source - rename fails, target with the same name exists
    await(fsStorage(root).storeFile(new File(s"$root/missing"), name)) shouldBe tailPath
    target.lastModified should be > oldTime + 24 * 3600 * 1000L
  }

  it should "fail to store file if move fails and file is not already stored" in {
    val root = newRoot
    an [RuntimeException] should be thrownBy
      await(fsStorage(root).storeFile(new File(s"$root/missing"), "b" * 64))
  }
}
