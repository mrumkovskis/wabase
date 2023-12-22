package org.wabase

import org.mojoz.metadata.in.YamlMd
import org.wabase.AppMetadata.{Action, DbAccessKey, JobDef}
import org.snakeyaml.engine.v2.api.LoadSettings
import org.snakeyaml.engine.v2.api.Load
import scala.collection.immutable._
import scala.jdk.CollectionConverters._


object YamlJobDefLoader {
  private val jobDefPattern       = "(^|\\n)job\\s*:".r // XXX
  private def isJobDef(d: YamlMd) = jobDefPattern.findFirstIn(d.body).isDefined
}

class YamlJobDefLoader(yamlMd: Seq[YamlMd]) {
  val sources = yamlMd.filter(YamlJobDefLoader.isJobDef)
  val rawJobDefs: Map[String, Map[String, Any]] = {
    def loadYamlRawJobDef(jobDefString: String, labelOrFilename: String = null, lineNumber: Int = 0) = {
      val loaderSettings = LoadSettings.builder()
        .setLabel(Option(labelOrFilename) getOrElse "wabase job metadata")
        .setAllowDuplicateKeys(false)
        .build();
      val lineNumberCorrection = if (lineNumber > 1) "\n" * (lineNumber - 1) else ""
      val jdMap =
        (new Load(loaderSettings)).loadFromString(lineNumberCorrection + jobDefString) match {
          case m: java.util.Map[String @unchecked, _] => m.asScala.toMap
          case x => sys.error(
            "Unexpected class: " + Option(x).map(_.getClass).orNull)
        }
      val jobName:      String  = jdMap.get("job").map(_.toString).getOrElse(sys.error("Missing job name"))
      jobName -> jdMap
    }
    val jds = sources map { jd =>
      try loadYamlRawJobDef(jd.body, jd.filename, jd.line) catch {
        case e: Exception => throw new RuntimeException(
          s"Failed to load job definition from ${jd.filename}, line ${jd.line}", e)
      }
    }
    val duplicateNames = jds.map(_._1).groupBy(identity).filter(_._2.size > 1).keys
    require(duplicateNames.isEmpty, "Duplicate job definitions: " + duplicateNames.mkString(", "))
    jds.toMap
  }
  def toJobDef(name: String, jdMap: Map[String, Any], parseAction: Map[String, Any] => Action): JobDef = {
    val steps: Action = parseAction(jdMap)
    val db: String = jdMap.get("db").map(_.toString).orNull
    val explicitDb: Boolean = jdMap.get("explicit db").exists(_.toString.toBoolean)
    val dbAccessKeys: Seq[DbAccessKey] = Nil
    JobDef(name, steps, db, explicitDb, dbAccessKeys)
  }
}
