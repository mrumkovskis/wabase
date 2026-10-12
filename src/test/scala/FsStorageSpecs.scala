package org.wabase

import com.typesafe.config.{ConfigException, ConfigFactory}
import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers

class FsStorageSpecs extends FlatSpec with Matchers {
  behavior of "FsStorage"

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
}
