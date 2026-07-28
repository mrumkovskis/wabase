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

  it should "sort response status codes after deep merge of path items" in {
    val lower = Json.mapper().readValue(
      """{"get":{"responses":{"404":{"description":"Not Found"},"503":{"description":"Unavailable"}}}}""",
      classOf[PathItem],
    )
    val higher = Json.mapper().readValue(
      """{"get":{"responses":{"200":{"description":"OK"},"400":{"description":"Bad Request"}}}}""",
      classOf[PathItem],
    )
    val merged = SwaggerMerger.mergePathItems(Seq(lower, higher))
    merged should have size 1
    merged.head.getGet.getResponses.keySet.asScala.toList shouldEqual List("200", "400", "404", "503")
  }

  it should "sort response status codes in mergePathSources without overrides" in {
    val paths = Seq(
      "/json-query-param" -> Json.mapper().readValue(
        """{"get":{"responses":{"404":{"description":"Not Found"},"503":{"description":"Unavailable"},"200":{"description":"OK"},"400":{"description":"Bad Request"}}}}""",
        classOf[PathItem],
      )
    )
    val merged = SwaggerMerger.mergePathSources(Seq((paths, new HashMap[String, Object]())))
    merged should have size 1
    merged.head._2.getGet.getResponses.keySet.asScala.toList shouldEqual List("200", "400", "404", "503")
  }

  for (doc <- docs) {
    val shouldStr = doc.get("should").asInstanceOf[String]
    val mode = Option(doc.get("mode")).map(_.toString).getOrElse("mergePaths")
    it should shouldStr in {
      mode match {
        case "mergePathItems" =>
          val pathItemsYaml = doc.get("path_items").asInstanceOf[JList[Object]]
          val pathItems = pathItemsYaml.asScala.map { itemYaml =>
            Json.mapper().readValue(Json.mapper().writeValueAsBytes(itemYaml), classOf[PathItem])
          }.toSeq
          val merged = SwaggerMerger.mergePathItems(pathItems)
          merged should have size 1
          val resultMap = Json.mapper().convertValue(merged.head, classOf[JMap[String, Object]])
          val expectedYaml = doc.get("expected_item").asInstanceOf[JMap[String, Object]]
          toSortedMap(resultMap).toString shouldEqual toSortedMap(expectedYaml).toString

        case "mergePathSources" =>
          val sourcesYaml = doc.get("sources").asInstanceOf[JList[Object]]
          val sources = sourcesYaml.asScala.map {
            case src: JMap[_, _] =>
              val srcMap = src.asInstanceOf[JMap[String, Object]]
              val pathsYaml = srcMap.get("paths").asInstanceOf[JMap[String, Object]]
              val basePaths = yamlToPaths(pathsYaml)
              val overrides = Option(srcMap.get("overrides")) match {
                case Some(m: JMap[_, _]) => m.asInstanceOf[JMap[String, Object]]
                case Some(null) | None => new HashMap[String, Object]()
                case Some(other) => throw new IllegalArgumentException(s"Unexpected overrides: $other")
              }
              (basePaths, overrides)
            case other => throw new IllegalArgumentException(s"Unexpected source: $other")
          }.toSeq
          val typeNameToSchema = typeNameToSchemaFrom(doc)
          val merged = SwaggerMerger.mergePathSources(sources, typeNameToSchema)
          val resultMap = pathsToResultMap(merged)
          val expectedYaml = doc.get("expected").asInstanceOf[JMap[String, Object]]
          toSortedMap(resultMap).toString shouldEqual toSortedMap(expectedYaml).toString

        case _ =>
          val pathsYaml = if (doc.containsKey("paths")) doc.get("paths").asInstanceOf[JMap[String, Object]] else null
          val basePaths = yamlToPaths(pathsYaml)
          val overrides = doc.get("overrides").asInstanceOf[JMap[String, Object]]
          val typeNameToSchema = typeNameToSchemaFrom(doc)
          val merged = SwaggerMerger.mergePaths(basePaths, overrides, typeNameToSchema)
          val resultMap = pathsToResultMap(merged)
          val expectedYaml = doc.get("expected").asInstanceOf[JMap[String, Object]]
          toSortedMap(resultMap).toString shouldEqual toSortedMap(expectedYaml).toString
      }
    }
  }

  private def typeNameToSchemaFrom(doc: JMap[String, Object]): String => Schema[_] = {
    val typeToSchemaYaml =
      if (doc.containsKey("type_to_schema")) doc.get("type_to_schema").asInstanceOf[JMap[String, Object]]
      else null
    if (typeToSchemaYaml == null) null
    else { name =>
      val schemaYaml = typeToSchemaYaml.get(name).asInstanceOf[JMap[String, Object]]
      if (schemaYaml == null) null
      else Json.mapper().readValue(Json.mapper().writeValueAsBytes(schemaYaml), classOf[Schema[_]])
    }
  }

  private def pathsToResultMap(merged: Seq[(String, PathItem)]): HashMap[String, Object] = {
    val resultMap = new HashMap[String, Object]()
    for ((path, item) <- merged) {
      resultMap.put(path, Json.mapper().convertValue(item, classOf[JMap[String, Object]]))
    }
    resultMap
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
