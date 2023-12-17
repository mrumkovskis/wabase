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

class YamlJobDefLoader(yamlMd: Seq[YamlMd], parseAction: Map[String, Any] => Action) {
  val sources = yamlMd.filter(YamlJobDefLoader.isJobDef)
  val jobDefs = {
    val rawJobDefs = sources map { jd =>
      try loadYamlJobDef(jd.body, jd.filename, jd.line) catch {
        case e: Exception => throw new RuntimeException(
          s"Failed to load job definition from ${jd.filename}, line ${jd.line}", e)
      }
    }
    val duplicateNames = rawJobDefs.map(_.name).groupBy(n => n).filter(_._2.size > 1).map(_._1)
    if (duplicateNames.size > 0)
      sys.error("Duplicate job definitions: " + duplicateNames.mkString(", "))
    rawJobDefs
  }
  private def loadYamlJobDef(jobDefString: String, labelOrFilename: String = null, lineNumber: Int = 0) = {
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
    val steps:        Action  = parseAction(jdMap)
    val db:           String  = jdMap.get("db").map(_.toString).orNull
    val explicitDb:   Boolean = jdMap.get("explicit db").map(_.toString.toBoolean).getOrElse(false)
    val dbAccessKeys: Seq[DbAccessKey] = Nil
    JobDef(jobName, steps, db, explicitDb, dbAccessKeys)
  }
}
