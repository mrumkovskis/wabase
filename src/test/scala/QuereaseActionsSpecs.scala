package org.wabase

class QuereaseActionsSpecs extends QuereaseBaseSpecs {

  override def beforeAll(): Unit = {
    querease = new QuereaseBase("/querease-action-specs-metadata.yaml")
    super.beforeAll()
  }
}
