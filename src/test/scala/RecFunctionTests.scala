package org.wabase


import org.scalatest.flatspec.AnyFlatSpec
import org.wabase.MapUtils._
import scala.annotation.tailrec
import scala.util.control.TailCalls._
import scala.language.postfixOps

class RecFunctionTests extends AnyFlatSpec {

  "The transform function" should "throw a StackOverflowError when given a very deep nested map" in {
    val path = ""
    val transformVal: Any => Any = identity
    val depth = 5999 //value to trigger the stack overflow error
    var map: Map[String, Any] = Map.empty
    (1 to depth).foreach { _ =>
      map = Map("key" -> map)
    }
    try {
      transform(path, transformVal, map)
      fail("Expected a StackOverflowError but none was thrown")
    } catch {
      case e: StackOverflowError => succeed
    }
  }


  "The transform_ss function" should "never throw StackOverflowError when given a very deep nested map" in {
    val path = ""
    val transformVal: Any => Any = identity
    val depth = 10000 //value to trigger the stack overflow error
    var map: Map[String, Any] = Map.empty
    (1 to depth).foreach { _ =>
      map = Map("key" -> map)
    }
    try {
      transform_ss(path, transformVal, map)
      info("path depth of 10000 did not trigger stack overflow")
      succeed
    } catch {
      case e: StackOverflowError => fail("Unexpected StackOverflowError")
    }
  }

}

