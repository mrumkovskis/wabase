package org.wabase

object MapUtils {
  def transform(path: String, transformVal: Any => Any, map: Map[String, Any]): Map[String, Any] = {
    def transform(
        path: String,
        map: Map[String, Any],
        currentPath: String = ""): Map[String, Any] = {
      map map {
        case (k, v) if "/" + path == currentPath + "/" + k => k -> transformVal(v)
        case (k, m: Map[String, Any] @unchecked) => k -> transform(path, m, currentPath + "/" + k)
        case (k, l: List[Map[String, Any] @unchecked]) =>
          k -> (l map (m => transform(path, m, currentPath + "/" + k)))
        case x => x
      }
    }
    transform(path, map, "")
  }

  def replace(path: String, value: Any, map: Map[String, Any]) =
    transform(
      path,
      _ => value,
      map)

  def flattenTree(map: Map[String, Any], keyFields: List[String] = Nil): Map[List[Any], Any] = {
    def getKey(v: Any, index: Int) = v match{
      case map: Map[String, Any] @unchecked if keyFields.exists(map.contains) => keyFields.find(map.contains).map(s => map(s)).get
      case a if keyFields.contains("#index") => index
      case a => a.hashCode
    }
    def flatenValue(v: Any): Map[List[Any], Any] = v match{
      case map: Map[String, Any] @unchecked => map.flatMap(kv => flatenValue(kv._2).map(kv2 => (kv._1 :: kv2._1, kv2._2)))
      case l: List[Any] => l.zipWithIndex.flatMap(v => flatenValue(v._1).map(kv2 => (getKey(v._1, v._2) :: kv2._1, kv2._2))).toMap
      case a => Map(Nil-> a)
    }
    flatenValue(map)
  }

  def zipMaps[T, K](map1: Map[T, K], map2: Map[T, K]) =
    map1.map{kv => (kv._1, (kv._2, map2.getOrElse(kv._1, null)))} ++
      map2.map{kv => (kv._1, (map1.getOrElse(kv._1, null), kv._2))}

  def flattenAndZipMaps(map1: Map[String, _], map2: Map[String, _], keyFields: List[String] = Nil) = zipMaps(flattenTree(map1, keyFields), flattenTree(map2, keyFields))
  def diffMaps(map1: Map[String, _], map2: Map[String, _], keyFields: List[String] = Nil) = flattenAndZipMaps(map1, map2, keyFields).filter(kv => kv._2._1 != kv._2._2)
  def jsonizeDiff(map: Map[List[Any], (Any, Any)]) = {
    implicit def orderLists[A <: List[Any]]: Ordering[A] = Ordering.by(l => l.toString)
    implicit def orderDifs[A <: (List[Any], (Any, Any))]: Ordering[A] = Ordering.by(_._1)
    map.toList.sorted.map(x => Map("path"-> x._1, "old_value"-> x._2._1, "new_value"-> x._2._2))
  }
}

object MapRecursiveExtensions {
  import scala.language.implicitConversions
  // path matcher support
  case class /(node: Any, item: Any){
    def /(i2: Any) = new /(this, i2)
    override def toString = node.toString + "/" + item
  }
  implicit class atRoot(s: String){
    def /(s2: Any) = new /(s, s2)
  }

  // map traveller
  type RecursiveMap = Map[String, Any]
  type TransformationFunction = PartialFunction[(Any, Any), Any]

  implicit class MapToRecursiveExtension(map: RecursiveMap){
    def recursiveMap(f: TransformationFunction): RecursiveMap = {
      val fullTransform = f.orElse[(Any, Any), Any]{case (k, a) => a}

      def transformValue(value: Any, path: Any): Any = {
        fullTransform((path, value)) match {
          case (newKey: String) / (m1: RecursiveMap @unchecked) => newKey / iterateMap(m1, path)
          case (newKey: String) / (l: List[Any]) => newKey / iterateList(l, path)
          case (newKey: String) / a => newKey / a
          case _ / _ => sys.error("Only string keys supported in transformation function result")
          case m1: RecursiveMap @unchecked => iterateMap(m1, path)
          case l: List[Any] => iterateList(l, path)
          case a => a
        }
      }

      def notNullPath(path: Any, item: Any) = if (path == null) item else /(path, item)

      def iterateMap(m: RecursiveMap, path: Any): RecursiveMap =
        m.map{case (k, v) => transformValue(v, notNullPath(path, k)) match {
          case (newKey: String) / newValue =>  (newKey, newValue)
          case newValue => (k, newValue)
        }}

      def iterateList(list: List[Any], path: Any):  List[Any] =
        list.zipWithIndex.map{case (v, i) => transformValue(v,notNullPath(path, i))match {
          case _ / _ => sys.error("""Position change in list is not supported. TransformationFunction should not return "key" / "value" on list items""")
          case newValue => newValue
        }}

      if(map == null) null else iterateMap(map, null)
    }
  }
}
