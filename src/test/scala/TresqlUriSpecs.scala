package org.wabase

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.{MatchResult, Matcher}
import org.scalatest.matchers.should.Matchers
import org.wabase.TresqlUri.{PrimitiveTresql, Tresql}

import scala.collection.immutable.ListMap

class TresqlUriSpecs extends AnyFlatSpec with Matchers {

  object TrUriMatchers {
    class UrisEquals(expectedUri: TresqlUri.TrUri) extends Matcher[TresqlUri.TrUri] {
      def apply(calculatedUri: TresqlUri.TrUri) = {
        val res = calculatedUri match {
          case cu: TresqlUri.Tresql => expectedUri match {
            case eu: TresqlUri.Tresql => cu.queryStringColIdx == eu.queryStringColIdx
            case _ => false
          }
          case cu: TresqlUri.PrimitiveTresql => expectedUri match {
            case eu: TresqlUri.PrimitiveTresql =>
              cu.hostInitPath == eu.hostInitPath && cu.path == eu.path && cu.params == eu.params
            case _ => false
          }
        }
        MatchResult(
          res,
          s"$calculatedUri does not matches expected $expectedUri",
          s"$calculatedUri matches expected $expectedUri"
        )
      }
    }

    def matchUri(expectedUri: TresqlUri.TrUri) = new UrisEquals(expectedUri)
  }

  import TrUriMatchers._

  it should "parse tresql uri" in {
    val parser = TestApp.qe.parser
    val tresqlUri = TestApp.qe.tresqlUri

    val samples: Map[String, TresqlUri.TrUri] = ListMap(
      "'/uri'" -> PrimitiveTresql("'/uri'",List(),ListMap(),null),
      "1" -> PrimitiveTresql("1",List(),ListMap(), null),
      "true" -> PrimitiveTresql("true",List(),ListMap(),null),
      "{ '/uri' }" -> PrimitiveTresql("'/uri'",List(),ListMap(),null),
      "null{ '/uri' }" -> PrimitiveTresql("'/uri'",List(),ListMap(),null),
      "{ '/uri', 'path1' }" -> PrimitiveTresql("'/uri'",List("'path1'"),ListMap(),null),
      "{ '/uri', 'path1', :path2 }" -> PrimitiveTresql("'/uri'",List("'path1'", ":path2"),ListMap(),null),
      "{ '/uri', '?' }" -> PrimitiveTresql("'/uri'",List(),ListMap(),null),
      "{ '/uri', '?', :p1 p1 }" -> PrimitiveTresql("'/uri'",List(),ListMap("p1" -> ":p1"),null),
      "{ '/uri', '?', :p1 p1, 'p2' p2 }" -> PrimitiveTresql("'/uri'",List(),ListMap("p1" -> ":p1", "p2" -> "'p2'"),null),
      "{ '/uri', 'path1', '?' }" -> PrimitiveTresql("'/uri'",List("'path1'"),ListMap(),null),
      "{ '/uri', 'path1', :path2, '?' }" -> PrimitiveTresql("'/uri'",List("'path1'", ":path2"),ListMap(),null),
      "{ '/uri', 'path1', '?', :p1 p1 }" -> PrimitiveTresql("'/uri'",List("'path1'"),ListMap("p1" -> ":p1"),null),
      "{ '/uri', 'path1', '?', :p1 p1, :p2 p2 }" -> PrimitiveTresql("'/uri'",List("'path1'"),ListMap("p1" -> ":p1", "p2" -> ":p2"),null),
      "{ '/uri', 'path1', 'path2', '?', :p1 p1, :p2 p2 }" -> PrimitiveTresql("'/uri'",List("'path1'", "'path2'"),ListMap("p1" -> ":p1", "p2" -> ":p2"),null),
      "'/uri' || :a" -> Tresql(null, -1),
      "{ '/uri' || :v }" -> Tresql(null,-1),
      "{ '/uri' || :v, 'path' }" -> Tresql(null,-1),
      "{ '/uri' || :v, '?', :v v }" -> Tresql(null,1),
      "{ '/uri', 'path' || 'x', '?', :v v }" -> Tresql(null,2),
      "[1, 2]" -> Tresql(null,-1),
      "[]a/b" -> Tresql(null,-1),
      "[]a/b {1, '?', :p p}" -> Tresql(null,1),
      "a{1, '?', :p p}" -> Tresql(null,1),
    )
    for {
      case (us, spu) <- samples
    } yield {
      tresqlUri.parse(parser.parseExp(us))(parser) should matchUri(spu)
    }
  }
}
