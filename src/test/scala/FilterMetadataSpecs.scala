package org.wabase

import akka.actor.ActorSystem
import org.mojoz.metadata.Type
import org.mojoz.querease.FilterType._
import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.tresql._
import org.wabase.AppBase.{FilterLabel, FilterParameter}


class FilterMetadataSpecs extends FlatSpec with QuereaseBaseSpecs {

  implicit protected var tresqlResources: Resources = _
  implicit val system = ActorSystem("serializer-streams-specs")
  implicit val executor = system.dispatcher
  var app: TestApp = _

  override def beforeAll(): Unit = {
    querease = new TestQuerease("/filter-metadata-specs-metadata.yaml") {
      override lazy val viewNameToClassMap = QuereaseActionsDtos.viewNameToClass
    }
    app = new TestApp {
      override protected def initQuerease: QE = querease
    }
    super.beforeAll()
  }

  behavior of "FilterMetadata"

  it should "provide valid filter metadata" in {
    app.filterParameters(querease.viewDef("filter_metadata_test_1")) shouldBe List(
      FilterParameter(
        name        = "name",
        table       = "person",
        label       = FilterLabel("Name","contains"),
        nullable    = true,
        required    = false,
        type_       = Type("string",Some(51),None,None,false),
        enum_       = null,
        refViewName = null,
        filterType  = ComparisonFilter("name","%~~%","name","?"),
      ),
      FilterParameter(
        name        = "sex",
        table       = "person",
        label       = FilterLabel("Sex",null),
        nullable    = true,
        required    = false,
        type_       = Type("string",Some(1),None,None,false),
        enum_       = List("M", "F"),
        refViewName = null,
        filterType  = IdentFilter("sex","sex","?"),
      ),
      FilterParameter(
        name        = "birthdate_from",
        table       = "person",
        label       = FilterLabel("Birthdate","from"),
        nullable    = true,
        required    = false,
        type_       = Type("date",None,None,None,false),
        enum_       = null,
        refViewName = null,
        filterType  = IntervalFilter("birthdate_from","?","<=","birthdate","<=","birthdate_to","?"),
      ),
      FilterParameter(
        name        = "birthdate_to",
        table       = "person",
        label       = FilterLabel("Birthdate","to"),
        nullable    = true,
        required    = false,
        type_       = Type("date",None,None,None,false),
        enum_       = null,
        refViewName = null,
        filterType  = IntervalFilter("birthdate_from","?","<=","birthdate","<=","birthdate_to","?"),
      ),
    )

    app.filterParameters(querease.viewDef("filter_metadata_test_2")) shouldBe List(
      FilterParameter(
        name        = "sex",
        table       = "person",
        label       = FilterLabel("Sex",null),
        nullable    = true,
        required    = false,
        type_       = Type("string",Some(1),None,None,false),
        enum_       = List("M", "F"),
        refViewName = null,
        filterType  = IdentFilter("p.sex","sex","?"),
      ),
      FilterParameter(
        name        = "birthdate",
        table       = "person",
        label       = FilterLabel("Birthdate",null),
        nullable    = true,
        required    = false,
        type_       = Type("date",None,None,None,false),
        enum_       = null,
        refViewName = null,
        filterType  = IdentFilter("p.birthdate","birthdate","?"),
      ),
      FilterParameter(
        name        = "last_modified_from",
        table       = "account",
        label       = FilterLabel("Last modified","from"),
        nullable    = true,
        required    = false,
        type_       = Type("dateTime",None,None,None,false),
        enum_       = null,
        refViewName = null,
        filterType  = IntervalFilter("last_modified_from","?","<=","ma_.last_modified","<=","last_modified_to","?"),
      ),
      FilterParameter(
        name        = "last_modified_to",
        table       = "account",
        label       = FilterLabel("Last modified","to"),
        nullable    = true,
        required    = false,
        type_       = Type("dateTime",None,None,None,false),
        enum_       = null,
        refViewName = null,
        filterType  = IntervalFilter("last_modified_from","?","<=","ma_.last_modified","<=","last_modified_to","?"),
      ),
      FilterParameter(
        name        = "balance",
        table       = "account",
        label       = FilterLabel("Balance",null),
        nullable    = true,
        required    = false,
        type_       = Type("decimal",None,Some(10),Some(2),false),
        enum_       = null,
        refViewName = null,
        filterType  = IdentFilter("ma_.balance","balance","?"),
      ),
    )

    app.filterParameters(querease.viewDef("filter_metadata_test_3")) shouldBe List(
      FilterParameter(
        name        = "ma_number",
        table       = "person",
        label       = FilterLabel("Ma number",null),
        nullable    = false,
        required    = true,
        type_       = Type("string",None,None,None,false),
        enum_       = null,
        refViewName = null,
        filterType  = null,
      ),
      FilterParameter(
        name        = "sex",
        table       = "person",
        label       = FilterLabel("Sex",null),
        nullable    = false,
        required    = true,
        type_       = Type("string",Some(1),None,None,false),
        enum_       = List("M", "F"),
        refViewName = null,
        filterType  = IdentFilter("p.sex","sex",""),
      ),
      FilterParameter(
        name        = "modified_before_time",
        table       = "account",
        label       = FilterLabel("Modified before time",null),
        nullable    = false,
        required    = true,
        type_       = Type("dateTime",None,None,None,false),
        enum_       = null,
        refViewName = null,
        filterType  = ComparisonFilter("ma_.last_modified","<","modified_before_time",""),
      ),
    )
  }
}
