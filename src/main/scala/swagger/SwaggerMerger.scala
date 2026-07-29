package org.wabase.swagger

import io.swagger.v3.core.util.Json
import io.swagger.v3.oas.models.media.Schema
import io.swagger.v3.oas.models.PathItem
import java.util.{ArrayList, HashMap, List => JList, Map => JMap}
import scala.jdk.CollectionConverters._

object SwaggerMerger {

  private val mapper = Json.mapper()

  private val typeSet = Set("boolean", "integer", "number", "string", "array", "object", "null")

  private val schemaKeys = Set("schema", "items", "additionalProperties", "not")

  private val listSchemaKeys = Set("allOf", "anyOf", "oneOf")

  private val httpMethods = Set("get", "post", "put", "delete", "options", "head", "patch", "trace")

  private def trimmedKey(k: String): String = if (k.endsWith(" =")) k.substring(0, k.length - 2).trim else k

  /** @return (realKey, isReplace) for override map keys; keys ending with ` =` are replace markers. */
  private def parseOverrideKey(k: String): (String, Boolean) =
    if (k != null && k.endsWith(" =")) (k.substring(0, k.length - 2).trim, true)
    else (k, false)

  private def isMediaTypeKey(k: String): Boolean = k.contains("/") && !k.startsWith("/")

  /**
   * Merges base Swagger paths with overrides.
   *
   * Each root key in `overrides` is classified independently and applied at its own level:
   * `paths` (document), path (`/…`), method (`get`/`post`/…), response (`200`/`default`/`2XX`), or operation
   * (any other key, applied under every HTTP method of every path).
   *
   * Keys ending with ` =` replace the subtree; otherwise they are deep-merged.
   * Replace with null (`"404 =": null`, `"get =": null`, `"/x =": null`) removes that key.
   * For parameters map form, `"limit =": null` removes parameters with that name (any `in`).
   * Plain `key: null` without ` =` does not remove.
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

    val keys = mergedMap.keySet.asScala.toList.sorted
    keys.map { path =>
      val pathMap = mergedMap.get(path)
      sortResponses(pathMap.asInstanceOf[JMap[String, Object]])
      val pathItem = mapper.convertValue(pathMap, classOf[PathItem])
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
        // Only update methods already present on each path — do not invent operations.
        val newMap = new HashMap[String, JMap[String, Object]](baseMap)
        for (entry <- newMap.entrySet.asScala) {
          val pMap = entry.getValue.asInstanceOf[JMap[String, Object]]
          val scopedOvr = new HashMap[String, Object]()
          for ((k, v) <- overrideMap.asScala) {
            val methodName = trimmedKey(k).toLowerCase()
            if (httpMethods.contains(methodName)) {
              if (pMap.containsKey(methodName)) scopedOvr.put(k, v)
            } else {
              scopedOvr.put(k, v)
            }
          }
          if (!scopedOvr.isEmpty) {
            entry.setValue(merge(pMap, scopedOvr, typeNameToSchema))
          }
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
      val (realKey, isReplace) = parseOverrideKey(key)

      // `key =: null` removes the key; plain `key: null` is a no-op (does not remove)
      if (isReplace && value == null) {
        result.remove(realKey)
      } else if (value == null) {
        // ignore
      } else if (realKey == "parameters") {
        applyParametersOverride(result, value, isReplace, typeNameToSchema)
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

  private def applyParametersOverride(
    result: JMap[String, Object],
    value: Object,
    isReplace: Boolean,
    typeNameToSchema: String => Schema[_],
  ): Unit = {
    if (isReplace && value == null) {
      result.remove("parameters")
      return
    }
    val baseParams =
      if (result.containsKey("parameters")) result.get("parameters").asInstanceOf[JList[Object]]
      else new ArrayList[Object]()
    val (namesToRemove, ovrParams) = normalizeParametersWithRemovals(value, typeNameToSchema)
    val afterRemove = removeParametersByName(baseParams, namesToRemove)
    val mergedParams =
      if (isReplace) ovrParams
      else mergeParameters(afterRemove, ovrParams, typeNameToSchema)
    result.put("parameters", mergedParams)
  }

  /**
   * Normalizes parameter overrides. Map keys ending with ` =` and a null value are
   * removals (by parameter name, any `in`); other entries become parameter objects.
   */
  private def normalizeParametersWithRemovals(
    value: Object,
    typeNameToSchema: String => Schema[_],
  ): (Set[String], JList[Object]) = {
    value match {
      case m: JMap[_, _] =>
        val namesToRemove = scala.collection.mutable.LinkedHashSet[String]()
        val restMap = new HashMap[String, Object]()
        for ((k, v) <- m.asScala) {
          val (realKey, isReplace) = parseOverrideKey(k.asInstanceOf[String])
          if (isReplace && v == null) namesToRemove += realKey
          else restMap.put(realKey, v.asInstanceOf[Object])
        }
        (namesToRemove.toSet, normalizeParameters(restMap, typeNameToSchema))
      case other =>
        (Set.empty[String], normalizeParameters(other, typeNameToSchema))
    }
  }

  private def removeParametersByName(params: JList[Object], names: Set[String]): JList[Object] = {
    if (names.isEmpty) params
    else {
      val kept = new ArrayList[Object]()
      for (p <- params.asScala) {
        val pMap = p.asInstanceOf[JMap[String, Object]]
        val pName = pMap.get("name").asInstanceOf[String]
        if (!names.contains(pName)) kept.add(p)
      }
      kept
    }
  }

  private def normalizeParameters(value: Object, typeNameToSchema: String => Schema[_]): JList[Object] = {
    val rawList = value match {
      case l: JList[_] => l.asInstanceOf[JList[Object]]
      case m: JMap[_, _] =>
        val list = new ArrayList[Object]()
        for ((k, v) <- m.asScala) {
          val (name, _) = parseOverrideKey(k.asInstanceOf[String])
          val paramMap = new HashMap[String, Object]()
          paramMap.put("name", name)
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
    if (overrides == null || overrides.isEmpty) schema
    else {
      val schemaMap = mapper.convertValue(schema, classOf[JMap[String, Object]])
      val mergedMap = merge(schemaMap, overrides, typeNameToSchema, false, true)
      mapper.convertValue(mergedMap, classOf[Schema[_]])
    }
  }

  /**
   * Deep-merges path items into a single item. Later items win on conflicting keys
   * (use view after route so views take priority). Returns empty seq if input is empty,
   * otherwise a single-element seq.
   */
  def mergePathItems(pathItems: Seq[PathItem]): Seq[PathItem] = {
    if (pathItems.isEmpty) Seq()
    else Seq(pathItems.reduceLeft(deepMergePathItem))
  }

  private def deepMergePathItem(lower: PathItem, higher: PathItem): PathItem = {
    val lowerMap = mapper.convertValue(lower, classOf[JMap[String, Object]])
    val higherMap = mapper.convertValue(higher, classOf[JMap[String, Object]])
    // `merge` treats the second map as overrides: higher priority wins
    val mergedMap = merge(lowerMap, higherMap, null)
    sortResponses(mergedMap)
    mapper.convertValue(mergedMap, classOf[PathItem])
  }

  private def sortPathItemResponses(pathItem: PathItem): PathItem = {
    val pathMap = mapper.convertValue(pathItem, classOf[JMap[String, Object]])
    sortResponses(pathMap)
    mapper.convertValue(pathMap, classOf[PathItem])
  }

  private def emptyOverrides: JMap[String, Object] = new HashMap[String, Object]()

  private def partitionPathsLevelOverrides(overrides: JMap[String, Object]): (JMap[String, Object], JMap[String, Object]) = {
    val pathsLevel = new HashMap[String, Object]()
    val rest = new HashMap[String, Object]()
    if (overrides != null) {
      for ((k, v) <- overrides.asScala) {
        if (isPathsKey(k)) pathsLevel.put(k, v)
        else rest.put(k, v)
      }
    }
    (pathsLevel, rest)
  }

  private def pathItemHttpMethods(item: PathItem): Set[String] = {
    val map = mapper.convertValue(item, classOf[JMap[String, Object]])
    httpMethods.filter(map.containsKey)
  }

  /** Keeps path-level (non-method) keys and only the given HTTP methods. */
  private def filterPathItemToMethods(item: PathItem, methods: Set[String]): PathItem = {
    val map = mapper.convertValue(item, classOf[JMap[String, Object]])
    val filtered = new HashMap[String, Object]()
    for ((k, v) <- map.asScala) {
      if (!httpMethods.contains(k) || methods.contains(k)) filtered.put(k, v)
    }
    mapper.convertValue(filtered, classOf[PathItem])
  }

  /**
   * Writes override result `updated` into `current` for `methodScope` only.
   * Methods removed by overrides are those present on the scoped slice before
   * overrides (`methodsBefore`) but missing afterwards — not merely absent from
   * a partial path-level patch.
   */
  private def spliceScopedPathItem(
    current: PathItem,
    updated: PathItem,
    methodScope: Set[String],
    methodsBefore: Set[String],
  ): PathItem = {
    val cur = mapper.convertValue(current, classOf[JMap[String, Object]])
    val upd = mapper.convertValue(updated, classOf[JMap[String, Object]])
    val result = new HashMap[String, Object](cur)
    val methodsAfter = httpMethods.filter(upd.containsKey)

    for (m <- methodsBefore.intersect(methodScope) if !methodsAfter.contains(m)) {
      result.remove(m)
    }
    for (m <- methodsAfter if methodScope.contains(m)) {
      result.put(m, upd.get(m))
    }
    // Path-level keys (parameters, summary, …) from the scoped merge
    for ((k, v) <- upd.asScala if !httpMethods.contains(k)) {
      result.put(k, v)
    }
    sortPathItemResponses(mapper.convertValue(result, classOf[PathItem]))
  }

  private def pathItemHasHttpMethod(item: PathItem): Boolean =
    pathItemHttpMethods(item).nonEmpty

  /**
   * Keep only override roots that apply to this source's contributed paths.
   * Path-level keys for paths this source did not generate are dropped (e.g. swagger
   * inherited via view `extends` must not recreate/overwrite another view's path).
   */
  private def scopeOverridesToContributedPaths(
    overrides: JMap[String, Object],
    contributedPathNames: Set[String],
  ): JMap[String, Object] = {
    val scoped = new HashMap[String, Object]()
    for ((k, v) <- overrides.asScala) {
      if (isPathKey(k)) {
        if (contributedPathNames.contains(trimmedKey(k))) scoped.put(k, v)
      } else {
        scoped.put(k, v)
      }
    }
    scoped
  }

  /**
   * Merges path contributions from multiple sources in ascending priority order
   * (last source wins).
   *
   * 1. `paths` / `paths =` decide each source's contributed paths (`paths =: {}` → none).
   * 2. Contributed bases are deep-merged (later source wins on conflicts).
   * 3. Each source's remaining overrides are applied only to that source's
   *    **paths and HTTP methods**, then written back without touching other
   *    sources' operations. Path-level override keys for paths not contributed by
   *    this source are ignored (avoids inherited swagger wiping another view's path).
   *
   * @param sources sequence of (base paths, overrides), low priority first
   * @param typeNameToSchema optional custom type resolver for overrides
   */
  def mergePathSources(
    sources: Seq[(Seq[(String, PathItem)], JMap[String, Object])],
    typeNameToSchema: String => Schema[_] = null,
  ): Seq[(String, PathItem)] = {
    if (sources.isEmpty) Seq.empty
    else {
      val prepared: Seq[(Seq[(String, PathItem)], JMap[String, Object])] = sources.map { case (basePaths, overrides) =>
        val ovr = if (overrides == null) emptyOverrides else overrides
        val (pathsLevel, rest) = partitionPathsLevelOverrides(ovr)
        val contributed =
          if (pathsLevel.isEmpty) basePaths
          else mergePaths(basePaths, pathsLevel, typeNameToSchema)
        (contributed, rest)
      }

      val resultMap = new java.util.LinkedHashMap[String, PathItem]()
      for ((paths, _) <- prepared) {
        for ((name, item) <- paths) {
          if (resultMap.containsKey(name)) {
            resultMap.put(name, deepMergePathItem(resultMap.get(name), item))
          } else {
            resultMap.put(name, item)
          }
        }
      }

      for ((paths, rest) <- prepared) {
        if (rest != null && !rest.isEmpty && paths.nonEmpty) {
          val sourceMethodsByPath: Map[String, Set[String]] =
            paths.map { case (name, item) => name -> pathItemHttpMethods(item) }.toMap
          val contributedNames = sourceMethodsByPath.keySet
          val scopedRest = scopeOverridesToContributedPaths(rest, contributedNames)
          if (scopedRest.isEmpty) {
            // nothing applicable to this source
          } else {
            // Slice current merge to this source's paths + methods only
            val subset = paths.flatMap { case (name, _) =>
              Option(resultMap.get(name)).map { current =>
                name -> filterPathItemToMethods(current, sourceMethodsByPath(name))
              }
            }
            val methodsBeforeByPath: Map[String, Set[String]] =
              subset.map { case (name, item) => name -> pathItemHttpMethods(item) }.toMap
            val subsetNames = methodsBeforeByPath.keySet

            val after = mergePaths(subset, scopedRest, typeNameToSchema)
            val afterMap = after.toMap

            // Write back updated paths (scoped splice — never drop sibling methods)
            for ((name, updated) <- after) {
              if (subsetNames.contains(name)) {
                val current = resultMap.get(name)
                if (current != null) {
                  resultMap.put(
                    name,
                    spliceScopedPathItem(
                      current,
                      updated,
                      sourceMethodsByPath(name),
                      methodsBeforeByPath.getOrElse(name, Set.empty),
                    ),
                  )
                } else {
                  resultMap.put(name, sortPathItemResponses(updated))
                }
              } else if (resultMap.containsKey(name)) {
                // Should be rare after path scoping; never replace an existing path wholesale
                resultMap.put(name, deepMergePathItem(resultMap.get(name), updated))
              } else {
                resultMap.put(name, sortPathItemResponses(updated))
              }
            }

            // Path removed only if it was in the scoped input but not in mergePaths output
            for (name <- subsetNames if !afterMap.contains(name)) {
              Option(resultMap.get(name)).foreach { current =>
                val remaining = filterPathItemToMethods(
                  current,
                  pathItemHttpMethods(current) -- sourceMethodsByPath.getOrElse(name, Set.empty),
                )
                if (pathItemHasHttpMethod(remaining)) resultMap.put(name, remaining)
                else resultMap.remove(name)
              }
            }
          }
        }
      }

      resultMap.asScala.toSeq.sortBy(_._1).map { case (name, item) =>
        name -> sortPathItemResponses(item)
      }
    }
  }
}
