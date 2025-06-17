package org.wabase

import MapUtils._

import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers


class MapDiffTests extends FlatSpec with Matchers {

  "flattenTree" should "work" in {
    flattenTree(Map()) should be(Map())
    flattenTree(Map("a" -> "b")) should be(Map(List("a") -> "b"))
    flattenTree(Map("a" -> "b", "c" -> "d")) should be(Map(List("a") -> "b", List("c") -> "d"))
    flattenTree(Map("a" -> 1)) should be(Map(List("a") -> 1))

    flattenTree(Map("a" -> Map("b" -> "c"))) should be(Map(List("a", "b") -> "c"))
    flattenTree(Map("a" -> Map("b" -> "c"), "c" -> "d")) should be(Map(List("a", "b") -> "c", List("c") -> "d"))
    flattenTree(Map("a" -> Map("b" -> Map("c" -> "f")), "c" -> "d")) should be(Map(List("a", "b", "c") -> "f", List("c") -> "d"))

    flattenTree(Map("a" -> List("b"))) should be(Map(List("a", 98) -> "b"))
    flattenTree(Map("a" -> List("b", "c"))) should be(Map(List("a", 98) -> "b", List("a", 99) -> "c"))
    val hash = Map("b" -> "c", "m" -> "n").hashCode()
    flattenTree(Map("a" -> List(Map("b" -> "c", "m" -> "n")))) should be(Map(List("a", hash, "b") -> "c", List("a", hash, "m") -> "n"))
    flattenTree(Map("i" -> Map("a" -> List(Map("b" -> "c", "m" -> "n"))))) should be(Map(List("i", "a", hash, "b") -> "c", List("i", "a", hash, "m") -> "n"))
  }
    "The flattenTree function" should "never throw StackOverflowError when given a very deep nested map or list" in {

      val testCapabilityDepth = 10000
      val constructionDepth = 5000 // A value that should not cause SOE during Map construction
      info(s"flattenTree_ss Deep Map Test: Construction Depth $constructionDepth, Testing Function Capability up to $testCapabilityDepth")
      val innermostLeafMap: Map[String, Any] = Map("key" -> "leaf_value")
      val actualDeepMap: Map[String, Any] = (1 to constructionDepth - 1).foldLeft(innermostLeafMap) { (acc, _) =>
        Map("key" -> acc)
      }

      try {
        val expectedPathForDeepMap = List.fill(constructionDepth)("key")
        val expectedDeepMapResult = Map(expectedPathForDeepMap -> "leaf_value")

        flattenTree(actualDeepMap) should be(expectedDeepMapResult)
        info(s"Deep map flattenTree with construction depth $constructionDepth did not trigger stack overflow and produced expected result.")
        succeed
      } catch {
        case e: StackOverflowError => fail("Unexpected StackOverflowError for deep map: " + e.getMessage)
        case e: Exception => fail(s"Unexpected exception for deep map: ${e.getMessage}")
      }

      info(s"flattenTree Deep List of Maps Test: Construction Depth $constructionDepth, Testing Function Capability up to $testCapabilityDepth")
      val innermostList: List[Map[String, Any]] = List(Map("item1" -> "val1"), Map("item2" -> "val2"))
      val actualDeepListMapInput: Map[String, Any] = (1 to constructionDepth).foldLeft(innermostList: Any) { (acc: Any, i) =>
        Map(s"key_$i" -> acc)
      }.asInstanceOf[Map[String, Any]]

      try {
        val result = flattenTree(actualDeepListMapInput, List("#index"))
        info(s"Deep list of maps (as leaf of deep map) flattenTree with construction depth $constructionDepth did not trigger stack overflow. Result size: ${result.size}")
        succeed
      } catch {
        case e: StackOverflowError => fail("Unexpected StackOverflowError for deep list of maps (as leaf): " + e.getMessage)
        case e: Exception => fail(s"Unexpected exception for deep list of maps (as leaf): ${e.getMessage}")
      }

      info(s"flattenTree Deep List of Primitives Test: Construction Depth $constructionDepth, Testing Function Capability up to $testCapabilityDepth")
      val deepListPrimitives: List[Any] = (1 to constructionDepth).toList
      val rootMapForList = Map("numbers" -> deepListPrimitives)
      try {
        val result = flattenTree(rootMapForList, List("#index"))
        info(s"Deep list of primitives flattenTree with construction depth $constructionDepth did not trigger stack overflow. Result size: ${result.size}")
        succeed
      } catch {
        case e: StackOverflowError => fail("Unexpected StackOverflowError for deep list of primitives: " + e.getMessage)
        case e: Exception => fail(s"Unexpected exception for deep list of primitives: ${e.getMessage}")
      }
    }

  "The flattenTree_ss function" should "work with various map and list structures and keying strategies" in {
    // Test Case 1: Empty map
    info("flattenTree_ss Test Case 1: Empty map")
    flattenTree_ss(Map()) should be(Map())

    // Test Case 2: Single key-value pair
    info("flattenTree_ss Test Case 2: Single key-value pair")
    flattenTree_ss(Map("a"->"b")) should be(Map(List("a")->"b"))

    // Test Case 3: Multiple key-value pairs at the root
    info("flattenTree_ss Test Case 3: Multiple key-value pairs at root")
    flattenTree_ss(Map("a"->"b","c"->"d")) should be(Map(List("a")->"b",List("c")->"d"))

    // Test Case 4: Single integer value
    info("flattenTree_ss Test Case 4: Single integer value")
    flattenTree_ss(Map("a"->1)) should be(Map(List("a")->1))

    // Test Case 5: Simple nested map
    info("flattenTree_ss Test Case 5: Simple nested map")
    flattenTree_ss(Map("a"->Map("b"->"c"))) should be(Map(List("a","b")->"c"))

    // Test Case 6: Nested map with a sibling key
    info("flattenTree_ss Test Case 6: Nested map with a sibling key")
    flattenTree_ss(Map("a"->Map("b"->"c"),"c"->"d")) should be(Map(List("a","b")->"c",List("c")->"d"))

    // Test Case 7: Deeply nested map with a sibling key
    info("flattenTree_ss Test Case 7: Deeply nested map with a sibling key")
    flattenTree_ss(Map("a"->Map("b"-> Map("c"->"f")),"c"->"d")) should be(Map(List("a","b","c")->"f",List("c")->"d"))

    // Test Case 8: List containing a single primitive value (default keyFields: hashCode)
    info("flattenTree_ss Test Case 8: List with single primitive (hashCode key)")
    flattenTree_ss(Map("a"->List("b"))) should be(Map(List("a","b".hashCode())->"b"))

    // Test Case 9: List containing multiple primitive values (default keyFields: hashCode)
    info("flattenTree_ss Test Case 9: List with multiple primitives (hashCode keys)")
    flattenTree_ss(Map("a"->List("b", "c"))) should be(Map(List("a","b".hashCode())->"b",List("a","c".hashCode())->"c"))

    // Test Case 10: List containing a nested map (default keyFields: hashCode for map object)
    info("flattenTree_ss Test Case 10: List with nested map (hashCode key for map)")
    val nestedMap1 = Map("b"->"c", "m"->"n")
    flattenTree_ss(Map("a"->List(nestedMap1))) should be(Map(List("a", nestedMap1.hashCode(),"b")->"c",List("a", nestedMap1.hashCode(),"m")->"n"))

    // Test Case 11: Deeply nested structure involving maps and lists (default keyFields: hashCode)
    info("flattenTree_ss Test Case 11: Deeply nested map-list structure (hashCode keys)")
    val nestedMap2 = Map("b"->"c", "m"->"n")
    flattenTree_ss(Map("i" ->Map("a"->List(nestedMap2)))) should be(Map(List("i","a",nestedMap2.hashCode(),"b")->"c",List("i","a",nestedMap2.hashCode(),"m")->"n"))

    // 12. List with "#index" in keyFields
    info("flattenTree_ss Test Case 12: List with '#index' in keyFields")
    flattenTree_ss(Map("items" -> List("apple", "banana")), List("#index")) should be(
      Map(List("items", 0) -> "apple", List("items", 1) -> "banana")
    )

    // 13. List of maps with specified keyFields for inner maps
    info("flattenTree_ss Test Case 13: List of maps with specific 'id' keyField")
    flattenTree_ss(Map("users" -> List(Map("id" -> 1, "name" -> "Alice"), Map("id" -> 2, "name" -> "Bob"))), List("id")) should be(
      Map(
        List("users", 1, "id") -> 1, List("users", 1, "name") -> "Alice",
        List("users", 2, "id") -> 2, List("users", 2, "name") -> "Bob"
      )
    )

    // 14. List of maps with mixed `keyFields` (id and #index)
    info("flattenTree_ss Test Case 14: List of maps with 'id' and '#index' keyFields")
    flattenTree_ss(Map("data" -> List(Map("value" -> "a"), Map("id" -> "x", "value" -> "b"))), List("id", "#index")) should be(
      Map(
        List("data", 0, "value") -> "a", // "id" not found, falls back to #index (0)
        List("data", "x", "id") -> "x", List("data", "x", "value") -> "b" // "id" found as "x"
      )
    )

    // 15. Empty nested map
    info("flattenTree_ss Test Case 15: Empty nested map")
    flattenTree_ss(Map("config" -> Map.empty[String, Any])) should be(Map())

    // 16. Empty list
    info("flattenTree_ss Test Case 16: Empty list")
    flattenTree_ss(Map("items" -> List.empty[Any])) should be(Map())

    // 17. Map with non-collection value directly
    info("flattenTree_ss Test Case 17: Map with primitive value")
    flattenTree_ss(Map("status" -> true)) should be(Map(List("status") -> true))

    // 18. Complex nested structure with mixed types and keying
    info("flattenTree_ss Test Case 18: Complex nested structure with mixed types and keying")
    val complexMap = Map(
      "books" -> List(
        Map("isbn" -> "123", "title" -> "Book A", "authors" -> List(Map("name" -> "Author1"))),
        Map("isbn" -> "456", "title" -> "Book B")
      ),
      "version" -> 1.0
    )
    val complexKeyFields = List("isbn", "name", "#index")
    val expectedComplex = Map(
      List("books", "123", "isbn") -> "123",
      List("books", "123", "title") -> "Book A",
      List("books", "123", "authors", "Author1", "name") -> "Author1",
      List("books", "456", "isbn") -> "456",
      List("books", "456", "title") -> "Book B",
      List("version") -> 1.0
    )
    flattenTree_ss(complexMap, complexKeyFields) should be(expectedComplex)


    // 19. TODO

    // 20. Value is null at a leaf node
    info("flattenTree_ss Test Case 20: Null value at leaf node")
    flattenTree_ss(Map("settings" -> Map("timeout" -> null))) should be(Map(List("settings", "timeout") -> null))

    // 21. Value is None at a leaf node
    info("flattenTree_ss Test Case 21: None value at leaf node")
    flattenTree_ss(Map("data" -> Map("optional_field" -> None))) should be(Map(List("data", "optional_field") -> None))
  }

  "The flattenTree_ss function" should "never throw StackOverflowError when given a very deep nested map or list" in {

    val testCapabilityDepth = 10000

    val constructionDepth = 5000
    info(s"flattenTree_ss Deep Map Test: Construction Depth $constructionDepth, Testing Function Capability up to $testCapabilityDepth")
    // Explicitly define type to prevent type inference issues during folding
    val innermostLeafMap: Map[String, Any] = Map("key" -> "leaf_value")
    val actualDeepMap: Map[String, Any] = (1 to constructionDepth - 1).foldLeft(innermostLeafMap) { (acc, _) =>
      Map("key" -> acc)
    }

    try {
      val expectedPathForDeepMap = List.fill(constructionDepth)("key")
      val expectedDeepMapResult = Map(expectedPathForDeepMap -> "leaf_value")

      flattenTree_ss(actualDeepMap) should be(expectedDeepMapResult)
      info(s"Deep map flattenTree_ss with construction depth $constructionDepth did not trigger stack overflow and produced expected result.")
      succeed
    } catch {
      case e: StackOverflowError => fail("Unexpected StackOverflowError for deep map: " + e.getMessage)
      case e: Exception => fail(s"Unexpected exception for deep map: ${e.getMessage}")
    }

    // iteratively to avoid SOE during setup
    info(s"flattenTree_ss Deep List of Maps Test: Construction Depth $constructionDepth, Testing Function Capability up to $testCapabilityDepth")
    val innermostList: List[Map[String, Any]] = List(Map("item1" -> "val1"), Map("item2" -> "val2"))
    val actualDeepListMapInput: Map[String, Any] = (1 to constructionDepth).foldLeft(innermostList: Any) { (acc: Any, i) =>
      Map(s"key_$i" -> acc)
    }.asInstanceOf[Map[String, Any]]

    try {
      val result = flattenTree_ss(actualDeepListMapInput, List("#index"))
      info(s"Deep list of maps (as leaf of deep map) flattenTree_ss with construction depth $constructionDepth did not trigger stack overflow. Result size: ${result.size}")
      succeed
    } catch {
      case e: StackOverflowError => fail("Unexpected StackOverflowError for deep list of maps (as leaf): " + e.getMessage)
      case e: Exception => fail(s"Unexpected exception for deep list of maps (as leaf): ${e.getMessage}")
    }

    info(s"flattenTree_ss Deep List of Primitives Test: Construction Depth $constructionDepth, Testing Function Capability up to $testCapabilityDepth")
    val deepListPrimitives: List[Any] = (1 to constructionDepth).toList
    val rootMapForList = Map("numbers" -> deepListPrimitives)
    try {
      val result = flattenTree_ss(rootMapForList, List("#index"))
      info(s"Deep list of primitives flattenTree_ss with construction depth $constructionDepth did not trigger stack overflow. Result size: ${result.size}")
      succeed
    } catch {
      case e: StackOverflowError => fail("Unexpected StackOverflowError for deep list of primitives: " + e.getMessage)
      case e: Exception => fail(s"Unexpected exception for deep list of primitives: ${e.getMessage}")
    }
  }


  "zipMaps" should "work" in {
    zipMaps(Map(),Map()) should be(Map())
    zipMaps(Map(List("a")->"b"),Map(List("a")->"c")) should be(Map(List("a")->("b", "c")))
    zipMaps(Map(List("a")->"b"),Map()) should be(Map(List("a")->("b", null)))
    zipMaps(Map(),Map(List("a")->"b")) should be(Map(List("a")->(null, "b")))
    zipMaps(Map(List("a")->"b"),Map(List("c")->"d")) should be(Map(List("a")->("b", null),List("c")->(null, "d")))
  }

  "diffMaps" should "work" in {
    diffMaps(Map(),Map()) should be(Map())
    diffMaps(Map("a"->"b"),Map("a"->"c")) should be(Map(List("a")->("b", "c")))
    diffMaps(Map("a"->"b"),Map()) should be(Map(List("a")->("b", null)))
    diffMaps(Map(),Map("a"->"b")) should be(Map(List("a")->(null, "b")))
    diffMaps(Map("a"->"b"),Map("a"->"b")) should be(Map())

    diffMaps(Map("a"->Map("b"-> Map("c"->"d"))),Map("a"->Map("b"-> Map("c"->"d")))) should be(Map())
    diffMaps(Map("a"->Map("b"-> Map("c"->"d"))),Map("a"->Map("b"-> Map("c"->"e")))) should be(Map(List("a","b","c")->("d", "e")))

    diffMaps(Map("a"->List("b", "c")),Map("a"->List("b", "c"))) should be(Map())
    diffMaps(Map("a"->List("b", "c")),Map("a"->List("b", "e"))) should be(Map(List("a",99)->("c", null),List("a",101)->(null, "e")))

    val hash1 = Map("c"->"d", "e"->"f").hashCode()
    val hash2 = Map("c"->"d1", "e"->"f").hashCode()
    diffMaps(Map("a"->List(Map("c"->"d", "e"->"f"), "c")),Map("a"->List(Map("c"->"d", "e"->"f"), "c"))) should be(Map())
    diffMaps(Map("a"->List(Map("c"->"d", "e"->"f"), "c")),Map("a"->List(Map("c"->"d1", "e"->"f"), "c"))) should be(
          Map(List("a",hash1,"c") -> ("d",null),
              List("a",hash1,"e") -> ("f",null),
              List("a",hash2,"c") -> (null,"d1"),
              List("a",hash2,"e") -> (null,"f")))
  }
}
