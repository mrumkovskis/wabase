package org.wabase

import org.mojoz.metadata.in.YamlMd
import org.snakeyaml.engine.v2.api.{Load, LoadSettings}
import scala.jdk.CollectionConverters._

object YamlRawDefLoader {
  def rawDefs(defName: String, yamlMd: Seq[YamlMd], isDef: YamlMd => Boolean): Map[String, Map[String, Any]] = {
    val sources = yamlMd filter isDef
      def loadYamlRawDef(defString: String, labelOrFilename: String = null, lineNumber: Int = 0) = {
      val loaderSettings = LoadSettings.builder()
        .setLabel(Option(labelOrFilename) getOrElse s"wabase $defName metadata")
        .setAllowDuplicateKeys(false)
        .build();
      val lineNumberCorrection = if (lineNumber > 1) "\n" * (lineNumber - 1) else ""
      val defMap =
        (new Load(loaderSettings)).loadFromString(lineNumberCorrection + defString) match {
          case m: java.util.Map[String @unchecked, _] => m.asScala.toMap
          case x => sys.error(
            "Unexpected class: " + Option(x).map(_.getClass).orNull)
        }
      val name: String  = defMap.get(defName).map(_.toString).getOrElse(sys.error(s"Missing $defName name"))
      name -> defMap
    }
    val ds = sources map { d =>
      try loadYamlRawDef(d.body, d.filename, d.line) catch {
        case e: Exception => throw new RuntimeException(
          s"Failed to load $defName definition from ${d.filename}, line ${d.line}", e)
      }
    }
    val duplicateNames = ds.map(_._1).groupBy(identity).filter(_._2.size > 1).keys
    require(duplicateNames.isEmpty, s"Duplicate $defName definitions: ${duplicateNames.mkString(", ")}")
    ds.toMap
  }
}
