package org.wabase

import MapUtils._

import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers


class TransformTests extends FlatSpec with Matchers {


  "transform" should "work" in {

    transform("", identity, Map()) should be(Map())

    transform("a/b/c", (_: Any) => "x", Map("a" -> Map("b" -> Map("c" -> "y")))) should be(Map("a" -> Map("b" -> Map("c" -> "x"))))

    transform("a/b/c/d", (_: Any) => "x", Map("a" -> Map("b" -> Map("c" -> Map("d" -> "y"))))) should be(Map("a" -> Map("b" -> Map("c" -> Map("d" -> "x")))))

    transform("a/b/c", (_: Any) => "x", Map("a" -> Map("b" -> List(Map("c" -> "y"), Map("c" -> "z"))))) should be(
      Map("a" -> Map("b" -> List(Map("c" -> "x"), Map("c" -> "x"))))
    )
    transform("a/b/c", (_: Any) => "x", Map("a" -> Map("b" -> Map("d" -> "y")))) should be(
      Map("a" -> Map("b" -> Map("d" -> "y")))
    )

    transform("a/b/c", (_: Any) => "x", Map("a" -> Map("b" -> List(Map("c"->"d", "e"->"f"), Map("g"->"h"))))) should be(
      Map("a" -> Map("b" -> List(Map("c" -> "x", "e" -> "f"), Map("g" -> "h"))))
    )

    transform("", (_: Any) => "x", Map()) should be(Map())
  }

  "The transform_ss function" should "work with various map structures and paths" in {
    // Test Case 1: Empty map and empty path - should return an empty map
    info("Test Case 1: Empty map and empty path")
    transform_ss("", identity, Map()) should be(Map())

    // Test Case 2: Simple transformation of a single value at a specific path
    info("Test Case 2: Simple transformation of a single value")
    transform_ss("a/b/c", (_: Any) => "x", Map("a" -> Map("b" -> Map("c" -> "y")))) should be(Map("a" -> Map("b" -> Map("c" -> "x"))))

    // Test Case 3: Transformation of a deeper nested map structure
    info("Test Case 3: Transformation of a deeper nested map structure")
    transform_ss("a/b/c/d", (_: Any) => "x", Map("a" -> Map("b" -> Map("c" -> Map("d" -> "y"))))) should be(Map("a" -> Map("b" -> Map("c" -> Map("d" -> "x")))))

    // Test Case 4: Transformation of values within a list of maps
    info("Test Case 4: Transformation of values within a list of maps")
    transform_ss("a/b/c", (_: Any) => "x", Map("a" -> Map("b" -> List(Map("c" -> "y"), Map("c" -> "z"))))) should be(
      Map("a" -> Map("b" -> List(Map("c" -> "x"), Map("c" -> "x"))))
    )

    // Test Case 5: Path does not match any existing key - no change expected
    info("Test Case 5: Ignore paths that don't match")
    transform_ss("a/b/c", (_: Any) => "x", Map("a" -> Map("b" -> Map("d" -> "y")))) should be(
      Map("a" -> Map("b" -> Map("d" -> "y")))
    )

    // Test Case 6: Transformation within a list of maps where some maps might not have the target key
    info("Test Case 6: Transformation in list of maps, with mixed matching/non-matching inner maps")
    transform_ss("a/b/c", (_: Any) => "x", Map("a" -> Map("b" -> List(Map("c"->"d", "e"->"f"), Map("g"->"h"))))) should be(
      Map("a" -> Map("b" -> List(Map("c" -> "x", "e" -> "f"), Map("g" -> "h"))))
    )

    // Test Case 7: Transforming a root-level key
    info("Test Case 7: Transforming a root-level key")
    transform_ss("root_key", (_: Any) => "new_value", Map("root_key" -> "old_value", "other_key" -> 123)) should be(
      Map("root_key" -> "new_value", "other_key" -> 123)
    )

    // Test Case 8: Path not found at any level (no change expected), more complex map
    info("Test Case 8: Path not found at any level (no change expected)")
    transform_ss("non_existent/path/to/value", (_: Any) => "transformed", Map("a" -> Map("b" -> "c"))) should be(
      Map("a" -> Map("b" -> "c"))
    )

    // Test Case 9: Transformation of a value to a different type
    info("Test Case 9: Transformation of a value to a different type")
    transform_ss("data/value", (_: Any) => 123.45, Map("data" -> Map("value" -> "hello"))) should be(
      Map("data" -> Map("value" -> 123.45))
    )

    // Test Case 10: Path leads through a list, but the target key is missing in some maps within the list
    info("Test Case 10: Path through list, target key missing in some inner maps")
    transform_ss("list_of_maps/target_key", (_: Any) => true,
      Map("list_of_maps" -> List(
        Map("other_key" -> 1, "target_key" -> false),
        Map("another_map_key" -> "abc"), // target_key is missing here
        Map("target_key" -> "maybe")
      ))
    ) should be(
      Map("list_of_maps" -> List(
        Map("other_key" -> 1, "target_key" -> true),
        Map("another_map_key" -> "abc"),
        Map("target_key" -> true)
      ))
    )

    // Test Case 11: Path points to a non-map/list value mid-way
    info("Test Case 11: Path points to non-map/list value mid-way")
    transform_ss("a/b/c/d", (_: Any) => "x", Map("a" -> Map("b" -> "not_a_map"))) should be(
      Map("a" -> Map("b" -> "not_a_map"))
    )

    // Test Case 12: Transforming a null value
    info("Test Case 12: Transforming a null value")
    transform_ss("config/param", (_: Any) => "default", Map("config" -> Map("param" -> null))) should be(
      Map("config" -> Map("param" -> "default"))
    )

    // Test Case 13: Transforming a None value
    info("Test Case 13: Transforming a None value")
    transform_ss("config/param", (_: Any) => "default", Map("config" -> Map("param" -> None))) should be(
      Map("config" -> Map("param" -> "default"))
    )

    // Test Case 14: Empty map within the path
    info("Test Case 14: Empty intermediate map")
    transform_ss("level1/level2/value", (_:Any) => 999, Map("level1" -> Map("level2" -> Map()))) should be(
      Map("level1" -> Map("level2" -> Map()))
    )
  }
}
