package org.wabase.swagger

import io.swagger.v3.core.util.Json
import io.swagger.v3.oas.models.PathItem
import io.swagger.v3.oas.models.media.Schema
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yaml.snakeyaml.Yaml
import java.io.File
import java.io.FileInputStream
import java.util.{ArrayList, HashMap, List => JList, Map => JMap, TreeMap}
import scala.jdk.CollectionConverters._

class SwaggerMergerSpecs extends AnyFlatSpec with Matchers {

  val yaml = new Yaml()
  val inputStream = new FileInputStream(new File("src/test/resources/swagger-merger-tests.yaml"))
  val docs = yaml.loadAll(inputStream).asScala.toList.filter(_ != null).asInstanceOf[List[JMap[String, Object]]]

  for (doc <- docs) {
    val shouldStr = doc.get("should").asInstanceOf[String]
    it should shouldStr in {
      val pathsYaml = if (doc.containsKey("paths")) doc.get("paths").asInstanceOf[JMap[String, Object]] else null
      val basePaths = yamlToPaths(pathsYaml)
      val overrides = doc.get("overrides").asInstanceOf[JMap[String, Object]]
      val typeToSchemaYaml = if (doc.containsKey("type_to_schema")) doc.get("type_to_schema").asInstanceOf[JMap[String, Object]] else null
      val typeNameToSchema: String => Schema[_] = if (typeToSchemaYaml == null) null else { name =>
        val schemaYaml = typeToSchemaYaml.get(name).asInstanceOf[JMap[String, Object]]
        if (schemaYaml == null) null else Json.mapper().readValue(Json.mapper().writeValueAsBytes(schemaYaml), classOf[Schema[_]])
      }
      val merged = SwaggerMerger.mergePaths(basePaths, overrides, typeNameToSchema)
      val resultMap = new HashMap[String, Object]()
      for ((path, item) <- merged) {
        resultMap.put(path, Json.mapper().convertValue(item, classOf[JMap[String, Object]]))
      }
      val expectedYaml = doc.get("expected").asInstanceOf[JMap[String, Object]]
      toSortedMap(resultMap).toString shouldEqual toSortedMap(expectedYaml).toString
    }
  }

  private def yamlToPaths(yaml: JMap[String, Object]): Seq[(String, PathItem)] = {
    if (yaml == null) Seq.empty else {
      val keys = yaml.keySet.asScala.toList.sorted
      keys.map { k =>
        (k, Json.mapper().readValue(Json.mapper().writeValueAsBytes(yaml.get(k)), classOf[PathItem]))
      }
    }
  }

  private def toSortedMap(map: JMap[String, Object]): TreeMap[String, Object] = {
    val tm = new TreeMap[String, Object]()
    for ((k, v) <- map.asScala) {
      tm.put(k, processValue(v))
    }
    tm
  }

  private def processValue(v: Object): Object = v match {
    case m: JMap[_, _] => toSortedMap(m.asInstanceOf[JMap[String, Object]])
    case l: JList[_] => new ArrayList[Object](l.asInstanceOf[JList[Object]].asScala.map(processValue).asJavaCollection)
    case other => other
  }
}
