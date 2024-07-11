package org.wabase

import org.mojoz.metadata.in.YamlMd
import org.wabase.AppMetadata.{Action, DbAccessKey, JobDef}

import scala.collection.immutable._

class YamlJobDefLoader(yamlMd: Seq[YamlMd], actionParser: String => String => Map[String, Any] => Action) {

  lazy val nameToJobDef: Map[String, JobDef] = {
    val ds = yamlMd.flatMap(_.parsed).filter(_ contains "job").map(s => s("job").toString -> s)
    val duplicateNames = ds.map(_._1).groupBy(identity).filter(_._2.size > 1).keys
    require(duplicateNames.isEmpty, s"Duplicate job definitions: ${duplicateNames.mkString(", ")}")
    ds.toMap.transform { (jobName, jdMap) =>
      val steps: Action = actionParser(jobName)("action")(jdMap)
      val db: String = jdMap.get("db").map(_.toString).orNull
      val explicitDb: Boolean = jdMap.get("explicit db").exists(_.toString.toBoolean)
      val dbAccessKeys: Seq[DbAccessKey] = Nil
      JobDef(jobName, steps, db, explicitDb, dbAccessKeys)
    }
  }
}
