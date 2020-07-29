package org.wabase

import java.io.{File, PrintWriter}
import java.nio.file.{Files, Paths}
import org.yaml.snakeyaml.{DumperOptions, Yaml}
import scala.collection.JavaConverters._
import scala.io.{Codec, Source}
import scala.language.{implicitConversions, reflectiveCalls}
import spray.json._
import MapRecursiveExtensions._

trait TemplateUtil { this: client.CoreClient =>

  import jsonConverter.MapJsonFormat

  val DEL = ".del"

  def resourcePath = "src/it/resources/"
  def getTemplatePath = new File(resourcePath + "templates")

  type MapTemplate = Map[String, Any]
  private case object NotDefined

  implicit def conversion[A, B >: AnyRef](map: Map[A, B]) = new {
    def zipWithMap[C >: AnyRef](second: Map[A, C], nullObject : AnyRef = null): Map[A, (B, C)] =
      map.map{
        case(k, null) => (k, (null, second.getOrElse[C](k, nullObject)))
        case(k, v) => (k, (v, second.getOrElse[C](k, nullObject)))
      } ++ second.map{
        case(k, null) => (k, (map.getOrElse[B](k, nullObject), null))
        case(k, v) => (k, (map.getOrElse[B](k, nullObject), v))
      }
  }

  implicit def mapShortcats(map: Map[String, Any]) = new {
    def a(param: String) = map(param).asInstanceOf[List[MapTemplate]]
    def s(param: String) = map(param).asInstanceOf[String]
    def s(param: String, defaultValue: String) = map.getOrElse(param, defaultValue).asInstanceOf[String]
    def m(param: String, defaultValue: Map[String, Any] = Map.empty) = map.getOrElse(param, defaultValue).asInstanceOf[Map[String, Any]]
    def b(param: String) = map.get(param).contains("true") || map.get(param).contains(true)
  }

  def pojoFromTemplate[T <: Dto](viewClass: Class[T], fileName: String) = viewClass.getConstructor().newInstance().fill(readPojoMap(new File(resourcePath + fileName), getTemplatePath).toJson.asJsObject)
  def readFileBytes(fileName: String): Array[Byte] = Files.readAllBytes(Paths.get(fileName))

  def mergeTemplate(template : MapTemplate, extras: MapTemplate): MapTemplate = template.zipWithMap(extras, nullObject = NotDefined) map {
    case (k, (v, NotDefined)) => k -> v
    case (k, (v1: MapTemplate @unchecked, v2: MapTemplate @unchecked)) => k -> mergeTemplate(v1, v2)
    case (k, (v1: List[MapTemplate] @unchecked, v2: List[MapTemplate] @unchecked))
      if (v1.nonEmpty && v1.head.isInstanceOf[MapTemplate]) ||
        (v2.nonEmpty && v2.head.isInstanceOf[MapTemplate])  => k -> v1.zip(v2).map(i => mergeTemplate(i._1, i._2))
    case (k, (v1: List[String] @unchecked, v2: List[String] @unchecked)) => k -> (v1 ++ v2)
    case (k, (_, v)) => k -> v
  }

  def cleanupTemplate(t: MapTemplate) = t.recursiveMap{case (_, m: Map[String, Any] @unchecked) => m.filter(_._2 != DEL)}.filter(_._2 != DEL)

  // subtracts source template map from target
  // deleteExtras = true would be correct to delete source template values,
  // but for response template subtracttion we assume that source template does not contain unnecessary values
  def subtractTemplateMap(source : MapTemplate, target: MapTemplate, deleteExtras: Boolean = false): MapTemplate = {
    def valPair (u: Any, v: Any) = if (u == v) null else v

    val full = source.zipWithMap(target, nullObject = NotDefined) map ( a => {
      val (k, tuple) = a
      val res = tuple match {
        case (v, NotDefined) => if (!deleteExtras) null
        else DEL
        case (NotDefined, v) => v
        case (u: String, v: String) => valPair(u, v)
        case (u: Integer, v: Integer) => valPair(u, v)
        case (u: Long, v: Long) => valPair(u, v)
        case (u: Boolean, v: Boolean) => valPair(u, v)
        case (v1: MapTemplate @unchecked, v2: MapTemplate @unchecked) =>
          val result = subtractTemplateMap(v1, v2)
          if (result.isEmpty) null else result
        case (v1: List[MapTemplate] @unchecked, v2: List[MapTemplate] @unchecked)
          if (v1.nonEmpty && v1.head.isInstanceOf[MapTemplate]) ||
            (v2.nonEmpty && v2.head.isInstanceOf[MapTemplate])  =>
          val l = v1.zip(v2).map(i => subtractTemplateMap(i._1, i._2))
          if (!l.exists(_.nonEmpty)) null else l
        case (v1: List[String] @unchecked, v2: List[String] @unchecked) => if (v1 == v2) null else v2
        case (_, v) => v
      }
      k -> res
    })

    full.filter(_._2 != null)
  }

 // def getTemplateFileName(path: String, spec: String): String = "%s/%s.yaml".format(path, spec)

  def readPojoMap(file: File, templateDir: File): MapTemplate = {
    def readTemplate(file: String) = readPojoMap(new File(templateDir.getAbsolutePath + "/" + file), templateDir)
    def process(m: MapTemplate): MapTemplate = (m.get("template") match {
      case None => m
      case Some(s: String) => mergeTemplate(readTemplate(s), m)
      case Some(s: List[String] @unchecked) => mergeTemplate(s.map(readTemplate).foldLeft(Map.empty: MapTemplate)(mergeTemplate), m)
      case Some(t) => throw new RuntimeException("Template reference not recognized: "+t)
    }).filter(i => i._1 != "template").map{
      case (k, null) => k -> null
      case (k, v: MapTemplate @unchecked) => k -> process(v)
      case (k, v: List[MapTemplate] @unchecked) if v.nonEmpty && v.head.isInstanceOf[MapTemplate] => k -> v.map(process)
      case (k, v) => k -> v
    }
    val content = Source.fromFile(file)(Codec.UTF8).mkString
    val yaml = (new Yaml).load(content)
    val map = javaMapToMap(yaml.asInstanceOf[java.util.Map[String, _]])
    val ret = process(map)
    ret
  }

  def subLevel1(m: MapTemplate): (String, MapTemplate) = {
    val keys = m.keys.filter(_.endsWith("Body"))
    if (keys.size != 1) null else {
      val key = keys.head
      m(key) match {
        case v: MapTemplate @unchecked => (key, v)
        case _ => throw new RuntimeException("template Body does not contain a Map!")
      }
    }
  }

  def subLevel(m: MapTemplate): (String, MapTemplate) =
    m.toList match {
      case (tuple :: Nil) => tuple match {
        case (k: String, v: MapTemplate @unchecked) => (k, v)
        case _ => null
      }
      case _ => null
    }

  // returns 2nd sublevel Map from source Map, and two outer keys
  // needed to strip outer tags, i.e. Body and save...Response
  def subLevel2(m: MapTemplate): (String, String, MapTemplate) = {
    val s1 = subLevel1(m)
    if (s1 == null) null
    else {
      val (k1, m1) = s1
      val s2 = subLevel(m1)
      if (s2 == null) null
      else {
        val (k2, m2) = s2
        (k1, k2, m2)
      }
    }
  }
/*
  def getResponseMap(f: String, reqMap: MapTemplate): MapTemplate = {
    val respMap = readPojoMap(f)
    if (!respMap.contains("merge_Body")) respMap
    else {
      val s1 = subLevel2(reqMap)
      val s2 = subLevel2(respMap)
      if (s1 == null || s2 == null) respMap
      else {
        val (x1, x2, m1) = s1
        val (k1, k2, m2) = s2
        val ret = Map("Body" -> Map(k2 -> mergeTemplate(m1, m2)))
        ret
      }
    }
  }*/

  def dumpYaml(m: MapTemplate, fileName: String) = {
    val options = new DumperOptions
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
    val yaml = new Yaml(options)

    val result = mapToJavaMap(m)
    val writer = new PrintWriter(new File(fileName))
    writer.write(yaml.dump(result))
    writer.close()
  }


  def mapToJavaMap(map: Map[String, _]):java.util.Map[String, _] = {
    val result = map.map { (entry: (String, _)) =>
      (entry._1,
        entry._2 match {
          case l: List[_] => listToJavaList(l)
          case m: Map[String@unchecked, _] => mapToJavaMap(m)
          case r => r
        }
      )
    }
    result.asInstanceOf[Map[String, _]].asJava
  }

  def  listToJavaList(list: List[_]): java.util.List[_] = {
    val result = list.toList.map {
      case l: List[_] => listToJavaList(l)
      case m: Map[String @unchecked, _] => mapToJavaMap(m)
      case r => r
    }
    result.asJava
  }

  def javaMapToMap(map: java.util.Map[String, _]):Map[String, _] = {
    val result = map.asScala.map(entry=>
      ( entry._1,
        entry._2 match{
          case l: java.util.List[_] => javaListToList(l)
          case m: java.util.Map[String @unchecked, _] => javaMapToMap(m)
          case r => r
        }
        )
    ).toMap
    result.asInstanceOf[Map[String, _]]
  }

  def  javaListToList(list: java.util.List[_]): List[_] = {
    val result = list.asScala.toList.map {
      case l: java.util.List[_] => javaListToList(l)
      case m: java.util.Map[String @unchecked, _] => javaMapToMap(m)
      case r => r
    }
    result
  }
}
