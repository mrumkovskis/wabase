package org.wabase

import org.mojoz.querease.TresqlMetadataFactory
import org.tresql.compiling.CompilerMetadata

class WabaseTresqlMetadataFactory extends TresqlMetadataFactory {
  override def create(conf: Map[String, String]): CompilerMetadata = {
    val enrichedConf =
      if (conf.contains("macros_class")) conf
      else TresqlResourcesConf.confs.get(null)
        .flatMap(c => Option(c.macros))
        .map(m => conf + ("macros_class" -> m.getClass.getName))
        .getOrElse(conf)
    super.create(enrichedConf)
  }
}
