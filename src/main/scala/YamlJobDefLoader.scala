package org.wabase

import org.mojoz.metadata.in.YamlMd
import org.wabase.AppMetadata.{Action, DbAccessKey, JobAct, JobDef}

import scala.collection.immutable._

class YamlJobDefLoader(yamlMd: Seq[YamlMd], actionParser: String => String => Map[String, Any] => Action) {

  lazy val nameToJobDef: Map[String, JobDef] = {
    YamlRawDefLoader.rawDefs(JobAct, yamlMd, isJobDef).transform { (jobName, jdMap) =>
      val steps: Action = actionParser(jobName)("action")(jdMap)
      val db: String = jdMap.get("db").map(_.toString).orNull
      val explicitDb: Boolean = jdMap.get("explicit db").exists(_.toString.toBoolean)
      val dbAccessKeys: Seq[DbAccessKey] = Nil
      JobDef(jobName, steps, db, explicitDb, dbAccessKeys)
    }
  }

  private val jobDefPattern       = "(^|\\n)job\\s*:".r // XXX
  private def isJobDef(d: YamlMd) = jobDefPattern.findFirstIn(d.body).isDefined
}
