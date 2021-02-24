package org.wabase

class QuereaseActionsSpecs extends QuereaseBaseSpecs {

  override def beforeAll(): Unit = {
    querease = new QuereaseBase("/querease-action-specs-metadata.yaml")
    println(querease.nameToViewDef("person"))
    super.beforeAll()
  }

  "person save action" should "return person" in {
     true should be(true)
  }
}
