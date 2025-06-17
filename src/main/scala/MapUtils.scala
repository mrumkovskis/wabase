package org.wabase
import scala.annotation.tailrec

object MapUtils {

  sealed trait TailRec[+A] {
    @tailrec final def run: A = this match {
      case Done(a) => a
      case Call(thunk) => thunk().run
    }

    def map[B](f: A => B): TailRec[B] = this match {
      case Done(a) => Done(f(a))
      case Call(thunk) => Call(() => thunk().map(f))
    }

    def flatMap[B](f: A => TailRec[B]): TailRec[B] = this match {
      case Done(a) => f(a)
      case Call(thunk) => Call(() => thunk().flatMap(f))
    }
  }

  case class Done[+A](a: A) extends TailRec[A]
  case class Call[+A](thunk: () => TailRec[A]) extends TailRec[A]

  def done[A](a: A): TailRec[A] = Done(a)
  def call[A](thunk: () => TailRec[A]): TailRec[A] = Call(thunk)

  def sequence[A](list: List[TailRec[A]]): TailRec[List[A]] =
    list.foldRight(Done(List.empty[A]): TailRec[List[A]]) { (currentTrampoline, accTrampoline) =>
      for {
        currentResult <- currentTrampoline
        accumulatedResults <- accTrampoline
      } yield currentResult :: accumulatedResults
    }

  def traverse[A, B](list: List[A])(f: A => TailRec[B]): TailRec[List[B]] = {
    list.foldRight(done(List.empty[B])) { (item, accTailRecList) =>
      for {
        b <- f(item)
        bs <- accTailRecList
      } yield b :: bs
    }
  }

  def transform_ss(path: String, transformVal: Any => Any, map: Map[String, Any]): Map[String, Any] = {

    def transformMapEntriesT(
                              remainingIterator: Iterator[(String, Any)],
                              currentPathPrefix: String,
                              acc: List[(String, Any)] // must be List to avoid Map's recursive additions
                            ): TailRec[Map[String, Any]] = call(() => {
      if (remainingIterator.hasNext) {
        val (k, v) = remainingIterator.next()
        val fullKeyPath = s"$currentPathPrefix/$k"

        val valueTransformation: TailRec[Any] =
          if (path == fullKeyPath.stripPrefix("/")) {
            done(transformVal(v))
          } else {
            transformAnyT(v, fullKeyPath)
          }
        valueTransformation.flatMap { transformedV =>
          transformMapEntriesT(
            remainingIterator,
            currentPathPrefix,
            (k -> transformedV) :: acc
          )
        }
      } else {
        done(acc.reverse.toMap)
      }
    })

    def transformAnyT(currentValue: Any, currentPathPrefix: String): TailRec[Any] = call(() => {
      // The `call` here ensures -- body is evaluated lazily.
      currentValue match {
        case m: Map[String, Any] @unchecked =>
          transformMapEntriesT(m.iterator, currentPathPrefix, List.empty[(String, Any)])

        case l: List[Map[String, Any] @unchecked] =>
          // Use 'traverse' to process each inner map in the list in a stack-safe manner.
          traverse(l) { innerMap =>
            transformAnyT(innerMap, currentPathPrefix).map(_.asInstanceOf[Map[String, Any]])
          }

        case other =>
          // If it's not a Map or List[Map], return the value wrapped in Done.
          done(other)
      }
    })

    transformAnyT(map, "").run.asInstanceOf[Map[String, Any]]
  }

  def flattenTree_ss(map: Map[String, Any], keyFields: List[String] = Nil): Map[List[Any], Any] = {

    def getKey(v: Any, index: Int): Any = v match {
      case m: Map[String, Any] @unchecked if keyFields.exists(m.contains) =>
        keyFields.find(m.contains).map(s => m(s)).get
      case _ if keyFields.contains("#index") => index
      case a => a.hashCode
    }

    def flatenMapEntriesAccumulatorT(
                                      remainingIterator: Iterator[(String, Any)],
                                      accMaps: List[Map[List[Any], Any]]
                                    ): TailRec[Map[List[Any], Any]] = call(() => {
      if (remainingIterator.hasNext) {
        val (k, v) = remainingIterator.next()
        flatenValueT(v).flatMap { innerFlattenedMap =>
          val transformedInner = innerFlattenedMap.map { case (path, value) =>
            (k :: path, value)
          }
          flatenMapEntriesAccumulatorT(remainingIterator, transformedInner :: accMaps)
        }
      } else {
        Done(accMaps.flatMap(identity).toMap)
      }
    })

    def flatenValueT(v: Any): TailRec[Map[List[Any], Any]] = call(() => {
      v match {
        case m: Map[String, Any] @unchecked =>
          // For maps, use the `flatenMapEntriesAccumulatorT` to process entries stack-safely.
          flatenMapEntriesAccumulatorT(m.iterator, List.empty)

        case l: List[Any] @unchecked =>
          // For lists, use the general `traverse` function for stack-safe iteration.
          traverse(l.zipWithIndex.toList) { case (item, idx) =>
            val itemKey = getKey(item, idx)
            flatenValueT(item).map { innerFlattenedMap =>
              innerFlattenedMap.map { case (path, value) =>
                (itemKey :: path, value)
              }
            }
          }.map { listOfMaps =>
            listOfMaps.flatMap(identity).toMap
          }

        case a => done(Map(Nil -> a))
      }
    })

    flatenValueT(map).run
  }

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
  case object Delete
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
        m.flatMap{case (k, v) => transformValue(v, notNullPath(path, k)) match {
          case (newKey: String) / newValue =>  List((newKey, newValue))
          case Delete => Nil
          case newValue => List((k, newValue))
        }}

      def iterateList(list: List[Any], path: Any):  List[Any] =
        list.zipWithIndex.flatMap{case (v, i) => transformValue(v,notNullPath(path, i))match {
          case _ / _ => sys.error("""Position change in list is not supported. TransformationFunction should not return "key" / "value" on list items""")
          case Delete => Nil
          case newValue => List(newValue)
        }}

      if(map == null) null else iterateMap(map, null)
    }
  }
}
