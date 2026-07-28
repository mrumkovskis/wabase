package org.wabase.swagger

import io.swagger.v3.core.util.Json
import io.swagger.v3.oas.models.media.Schema
import io.swagger.v3.oas.models.PathItem
import java.util.{ArrayList, HashMap, List => JList, Map => JMap}
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

object SwaggerMerger {

  private val mapper = Json.mapper()

  private val typeSet = Set("boolean", "integer", "number", "string", "array", "object", "null")

  private val schemaKeys = Set("schema", "items", "additionalProperties", "not")

  private val listSchemaKeys = Set("allOf", "anyOf", "oneOf")

  private val httpMethods = Set("get", "post", "put", "delete", "options", "head", "patch", "trace")

  private def trimmedKey(k: String): String = if (k.endsWith(" =")) k.substring(0, k.length - 2).trim else k

  private def isMediaTypeKey(k: String): Boolean = k.contains("/") && !k.startsWith("/")

  /**
   * Merges base Swagger paths with overrides.
   *
   * Each root key in `overrides` is classified independently and applied at its own level:
   * `paths` (document), path (`/…`), method (`get`/`post`/…), response (`200`/`default`/`2XX`), or operation
   * (any other key, applied under every HTTP method of every path). Keys ending with ` =` replace the subtree;
   * otherwise they are deep-merged.
   *
   * @param basePaths The base paths as a sequence of (path string, PathItem)
   * @param overrides The overrides as a Java Map loaded from YAML (via SnakeYAML)
   * @param typeNameToSchema Optional function to resolve custom type names to specific Schema subclasses
   * @return The merged paths as a sequence of (path string, PathItem)
   */
  def mergePaths(basePaths: Seq[(String, PathItem)], overrides: JMap[String, Object], typeNameToSchema: String => Schema[_] = null): Seq[(String, PathItem)] = {
    if (overrides.isEmpty) basePaths else mergePaths_(basePaths, overrides, typeNameToSchema)
  }

  private sealed trait OverrideLevel
  private case object PathsLevel extends OverrideLevel
  private case object PathLevel extends OverrideLevel
  private case object MethodLevel extends OverrideLevel
  private case object ResponseLevel extends OverrideLevel
  private case object OperationLevel extends OverrideLevel

  private def isPathKey(k: String): Boolean = trimmedKey(k).startsWith("/")

  private def isMethodKey(k: String): Boolean = httpMethods.contains(trimmedKey(k).toLowerCase())

  private def isResponseKey(k: String): Boolean = {
    val tk = trimmedKey(k)
    tk == "default" || tk.matches("""\d{3}""") || (tk.length == 3 && tk.charAt(0).isDigit && tk.substring(1).toLowerCase == "xx")
  }

  private def isPathsKey(k: String): Boolean = trimmedKey(k) == "paths"

  /** Detects the structural level of a single overrides root key. */
  private def detectOverrideLevel(k: String): OverrideLevel = {
    if (isPathsKey(k)) PathsLevel
    else if (isPathKey(k)) PathLevel
    else if (isMethodKey(k)) MethodLevel
    else if (isResponseKey(k)) ResponseLevel
    else OperationLevel
  }

  private def mergePaths_(basePaths: Seq[(String, PathItem)], overrides: JMap[String, Object], typeNameToSchema: String => Schema[_]): Seq[(String, PathItem)] = {
    val baseMap = new HashMap[String, JMap[String, Object]]()
    for ((path, item) <- basePaths) {
      baseMap.put(path, mapper.convertValue(item, classOf[JMap[String, Object]]))
    }

    var mergedMap: JMap[String, JMap[String, Object]] = baseMap
    for ((key, value) <- overrides.asScala) {
      val singleOverride = new HashMap[String, Object]()
      singleOverride.put(key, value)
      mergedMap = applyOverridesAtLevel(mergedMap, singleOverride, detectOverrideLevel(key), typeNameToSchema)
    }

    for (pathMap <- mergedMap.values.asScala) {
      sortResponses(pathMap.asInstanceOf[JMap[String, Object]])
    }

    val keys = mergedMap.keySet.asScala.toList.sorted
    keys.map { path =>
      val pathMap = mergedMap.get(path)
      val pathItem = mapper.readValue(mapper.writeValueAsBytes(pathMap), classOf[PathItem])
      (path, pathItem)
    }
  }

  private def applyOverridesAtLevel(
    baseMap: JMap[String, JMap[String, Object]],
    overrideMap: JMap[String, Object],
    level: OverrideLevel,
    typeNameToSchema: String => Schema[_],
  ): JMap[String, JMap[String, Object]] = level match {
    case PathsLevel =>
      val outerBase = new HashMap[String, Object]()
      outerBase.put("paths", baseMap)
      val mergedOuter = merge(outerBase, overrideMap, typeNameToSchema)
      mergedOuter.getOrDefault("paths", new HashMap[String, JMap[String, Object]]()).asInstanceOf[JMap[String, JMap[String, Object]]]
    case PathLevel =>
        merge(baseMap.asInstanceOf[JMap[String, Object]], overrideMap, typeNameToSchema).asInstanceOf[JMap[String, JMap[String, Object]]]
    case MethodLevel =>
        val newMap = new HashMap[String, JMap[String, Object]](baseMap)
        for (entry <- newMap.entrySet.asScala) {
          val pMap = entry.getValue.asInstanceOf[JMap[String, Object]]
          val mergedP = merge(pMap, overrideMap, typeNameToSchema)
          entry.setValue(mergedP)
        }
        newMap
    case ResponseLevel =>
        val newMap = new HashMap[String, JMap[String, Object]](baseMap)
        for (entry <- newMap.entrySet.asScala) {
          val pMap = entry.getValue.asInstanceOf[JMap[String, Object]]
          for (method <- httpMethods) {
            if (pMap.containsKey(method)) {
              val opMap = pMap.get(method).asInstanceOf[JMap[String, Object]]
              val responses = if (opMap.containsKey("responses")) {
                opMap.get("responses").asInstanceOf[JMap[String, Object]]
              } else {
                val newResponses = new HashMap[String, Object]()
                opMap.put("responses", newResponses)
                newResponses
              }
              val mergedResponses = merge(responses, overrideMap, typeNameToSchema)
              opMap.put("responses", mergedResponses)
            }
          }
        }
        newMap
    case OperationLevel =>
        val newMap = new HashMap[String, JMap[String, Object]](baseMap)
        for (entry <- newMap.entrySet.asScala) {
          val pMap = entry.getValue.asInstanceOf[JMap[String, Object]]
          for (method <- httpMethods) {
            if (pMap.containsKey(method)) {
              val opMap = pMap.get(method).asInstanceOf[JMap[String, Object]]
              val mergedOp = merge(opMap, overrideMap, typeNameToSchema)
              pMap.put(method, mergedOp)
            }
          }
        }
        newMap
  }

  private def sortResponses(map: JMap[String, Object]): Unit = {
    if (map.containsKey("responses")) {
      val responses = map.get("responses").asInstanceOf[JMap[String, Object]]
      val sortedResponses = new java.util.LinkedHashMap[String, Object]()
      responses.keySet.asScala.toList.sorted.foreach { k =>
        sortedResponses.put(k, responses.get(k))
      }
      map.put("responses", sortedResponses)
    }
    map.values.asScala.foreach {
      case subMap: JMap[_, _] => sortResponses(subMap.asInstanceOf[JMap[String, Object]])
      case _ =>
    }
  }

  private def merge(base: JMap[String, Object], ovr: JMap[String, Object], typeNameToSchema: String => Schema[_], isSchemaValues: Boolean = false, isSchemaMap: Boolean = false): JMap[String, Object] = {
    val result = new HashMap[String, Object](base)
    for ((key, value) <- ovr.asScala) {
      val (realKey, isReplace) = if (key.endsWith(" =")) {
        (key.substring(0, key.length - 2).trim, true)
      } else {
        (key, false)
      }

      if (realKey == "parameters") {
        val baseParams = if (result.containsKey("parameters")) result.get("parameters").asInstanceOf[JList[Object]] else new ArrayList[Object]()
        val ovrParams = normalizeParameters(value, typeNameToSchema)
        val mergedParams = if (isReplace) ovrParams else mergeParameters(baseParams, ovrParams, typeNameToSchema)
        result.put("parameters", mergedParams)
      } else {
        val newVal: Object = if (isReplace) {
          processValue(realKey, value, isSchemaValues, typeNameToSchema)
        } else if (result.containsKey(realKey)) {
          val baseVal = result.get(realKey)
          (baseVal, value) match {
            case (bMap: JMap[_, _], oMap: JMap[_, _]) if isMediaTypeKey(realKey) && oMap.asInstanceOf[JMap[String, Object]].keySet.asScala.exists(k => trimmedKey(k) == "type") =>
              val schemaMerged = merge(new HashMap[String, Object](), oMap.asInstanceOf[JMap[String, Object]], typeNameToSchema, false, true)
              val mediaOverride = new HashMap[String, Object]()
              mediaOverride.put("schema", schemaMerged)
              merge(bMap.asInstanceOf[JMap[String, Object]], mediaOverride, typeNameToSchema)
            case (bMap: JMap[_, _], oMap: JMap[_, _]) =>
              val subSchemaValues = (realKey == "properties")
              merge(bMap.asInstanceOf[JMap[String, Object]], oMap.asInstanceOf[JMap[String, Object]], typeNameToSchema, subSchemaValues, schemaKeys.contains(realKey) || isSchemaValues)
            case (bMap: JMap[_, _], oStr: String) if (schemaKeys.contains(realKey) || isSchemaValues) =>
              if (typeSet.contains(oStr)) createSchemaMap(oStr)
              else if (typeNameToSchema != null) mapper.convertValue(typeNameToSchema(oStr), classOf[JMap[String, Object]])
              else throw new IllegalArgumentException(s"Invalid schema type '$oStr'")
            case (bList: JList[_], oList: JList[_]) =>
              val newList = new ArrayList[Object](bList.asInstanceOf[JList[Object]])
              val processedOList = processList(realKey, oList.asInstanceOf[JList[Object]], typeNameToSchema)
              newList.addAll(processedOList)
              newList
            case _ => processValue(realKey, value, isSchemaValues, typeNameToSchema)
          }
        } else {
          processValue(realKey, value, isSchemaValues, typeNameToSchema)
        }
        result.put(realKey, newVal)
      }
    }
    if (isSchemaMap) {
      val typ = result.get("type")
      if (typ != null && typ.isInstanceOf[String]) {
        val s = typ.asInstanceOf[String]
        if (!typeSet.contains(s)) {
          if (typeNameToSchema != null) {
            val schemaMap = mapper.convertValue(typeNameToSchema(s), classOf[JMap[String, Object]])
            result.remove("type")
            schemaMap.putAll(result)
            return schemaMap
          } else {
            throw new IllegalArgumentException(s"Invalid schema type '$s'")
          }
        }
      }
    }
    result
  }

  private def normalizeParameters(value: Object, typeNameToSchema: String => Schema[_]): JList[Object] = {
    val rawList = value match {
      case l: JList[_] => l.asInstanceOf[JList[Object]]
      case m: JMap[_, _] =>
        val list = new ArrayList[Object]()
        for ((k, v) <- m.asScala) {
          val paramMap = new HashMap[String, Object]()
          paramMap.put("name", k.asInstanceOf[String])
          paramMap.put("in", "query") // default
          v match {
            case vm: JMap[_, _] =>
              paramMap.putAll(vm.asInstanceOf[JMap[String, Object]])
            case vs: String =>
              val schemaMap = processValue("schema", vs, true, typeNameToSchema).asInstanceOf[JMap[String, Object]]
              paramMap.put("schema", schemaMap)
            case _ =>
          }
          list.add(paramMap)
        }
        list
      case _ => new ArrayList[Object]()
    }
    val processedList = new ArrayList[Object]()
    for (param <- rawList.asScala) {
      val paramMap = param.asInstanceOf[JMap[String, Object]]
      if (paramMap.containsKey("type") && !paramMap.containsKey("schema")) {
        val typeVal = paramMap.remove("type")
        val schemaMap = new HashMap[String, Object]()
        schemaMap.put("type", typeVal)
        paramMap.put("schema", schemaMap)
      }
      processedList.add(paramMap)
    }
    processedList
  }

  private def mergeParameters(baseParams: JList[Object], ovrParams: JList[Object], typeNameToSchema: String => Schema[_]): JList[Object] = {
    val merged = new ArrayList[Object](baseParams)
    for (ovrParam <- ovrParams.asScala) {
      val ovrMap = ovrParam.asInstanceOf[JMap[String, Object]]
      val ovrName = ovrMap.get("name").asInstanceOf[String]
      val ovrIn = ovrMap.getOrDefault("in", "query").asInstanceOf[String]
      val foundIndex = merged.asScala.indexWhere { p =>
        val pMap = p.asInstanceOf[JMap[String, Object]]
        val pName = pMap.get("name").asInstanceOf[String]
        val pIn = pMap.getOrDefault("in", "query").asInstanceOf[String]
        pName == ovrName && pIn == ovrIn
      }
      if (foundIndex >= 0) {
        val baseParamMap = merged.get(foundIndex).asInstanceOf[JMap[String, Object]]
        val mergedParam = merge(baseParamMap, ovrMap, typeNameToSchema)
        merged.set(foundIndex, mergedParam)
      } else {
        merged.add(ovrParam)
      }
    }
    merged
  }

  private def processValue(realKey: String, value: Object, isSchemaValues: Boolean, typeNameToSchema: String => Schema[_]): Object = {
    val isSchemaPosition = schemaKeys.contains(realKey) || isSchemaValues
    value match {
      case oStr: String if isSchemaPosition =>
        if (typeSet.contains(oStr)) createSchemaMap(oStr)
        else if (typeNameToSchema != null) mapper.convertValue(typeNameToSchema(oStr), classOf[JMap[String, Object]])
        else throw new IllegalArgumentException(s"Invalid schema type '$oStr'")
      case m: JMap[_, _] =>
        if (isMediaTypeKey(realKey) && m.asInstanceOf[JMap[String, Object]].keySet.asScala.exists(k => trimmedKey(k) == "type")) {
          val schemaMerged = merge(new HashMap[String, Object](), m.asInstanceOf[JMap[String, Object]], typeNameToSchema, false, true)
          val mediaMap = new HashMap[String, Object]()
          mediaMap.put("schema", schemaMerged)
          mediaMap
        } else {
          val subSchemaValues = (realKey == "properties")
          val isSchemaMapHere = isSchemaPosition
          merge(new HashMap[String, Object](), m.asInstanceOf[JMap[String, Object]], typeNameToSchema, subSchemaValues, isSchemaMapHere)
        }
      case l: JList[_] =>
        processList(realKey, l.asInstanceOf[JList[Object]], typeNameToSchema)
      case other => other
    }
  }

  private def processList(realKey: String, l: JList[Object], typeNameToSchema: String => Schema[_]): JList[Object] = {
    val newList = new ArrayList[Object]()
    val isSchemaList = listSchemaKeys.contains(realKey)
    for (item <- l.asScala) {
      val processed = if (isSchemaList) {
        item match {
          case oStr: String =>
            if (typeSet.contains(oStr)) createSchemaMap(oStr)
            else if (typeNameToSchema != null) mapper.convertValue(typeNameToSchema(oStr), classOf[JMap[String, Object]])
            else throw new IllegalArgumentException(s"Invalid schema type '$oStr'")
          case im: JMap[_, _] => merge(new HashMap[String, Object](), im.asInstanceOf[JMap[String, Object]], typeNameToSchema, false, true)
          case other => other
        }
      } else {
        item match {
          case im: JMap[_, _] => merge(new HashMap[String, Object](), im.asInstanceOf[JMap[String, Object]], typeNameToSchema)
          case other => other
        }
      }
      newList.add(processed)
    }
    newList
  }

  private def createSchemaMap(t: String): JMap[String, Object] = {
    val m = new HashMap[String, Object]()
    m.put("type", t)
    if (t == "array") {
      val items = new HashMap[String, Object]()
      items.put("type", "object")
      m.put("items", items)
    }
    m
  }

  def mergeSchema(schema: Schema[_], overrides: JMap[String, Object], typeNameToSchema: String => Schema[_] = null): Schema[_] = {
    if (overrides.isEmpty) schema
    else {
      val schemaMap = mapper.convertValue(schema, classOf[JMap[String, Object]])
      val mergedMap = merge(schemaMap, overrides, typeNameToSchema, false, true)
      mapper.convertValue(mergedMap, classOf[Schema[_]])
    }
  }

  def mergePathItems(pathItems: Seq[PathItem]): Seq[PathItem] = {
    if (pathItems.isEmpty) Seq()
    else {
      val result = ListBuffer[PathItem]()
      var current = pathItems.head
      for (next <- pathItems.tail) {
        if (hasPathItemConflict(current, next)) {
          result += current
          current = next
        } else {
          current = mergePathItem(current, next)
        }
      }
      result += current
      result.toSeq
    }
  }

  private def hasPathItemConflict(a: PathItem, b: PathItem): Boolean = {
    val aMap = mapper.convertValue(a, classOf[JMap[String, Object]])
    val bMap = mapper.convertValue(b, classOf[JMap[String, Object]])
    val allKeys = aMap.keySet.asScala ++ bMap.keySet.asScala
    allKeys.exists { k =>
      hasConflict(aMap.get(k), bMap.get(k))
    }
  }

  private def mergePathItem(a: PathItem, b: PathItem): PathItem = {
    val aMap = mapper.convertValue(a, classOf[JMap[String, Object]])
    val bMap = mapper.convertValue(b, classOf[JMap[String, Object]])
    val mergedMap = new HashMap[String, Object](aMap)
    for ((k, v) <- bMap.asScala) {
      mergedMap.put(k, mergeObjects(mergedMap.get(k), v))
    }
    mapper.convertValue(mergedMap, classOf[PathItem])
  }

  private def hasConflict(a: Object, b: Object): Boolean = {
    if (a == null || b == null) false
    else if (a.isInstanceOf[String] || a.isInstanceOf[Number] || a.isInstanceOf[Boolean]) {
      !a.equals(b)
    } else true  // complex or list or map
  }

  private def mergeObjects(a: Object, b: Object): Object = {
    if (a == null) b
    else if (b == null) a
    else a // since equal or error, but checked
  }
}
