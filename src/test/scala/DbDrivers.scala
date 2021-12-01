package org.wabase

object DbDrivers {
  // Load jdbc drivers in one place sequentially to avoid deadlocks
  lazy val loadDrivers: List[Class[_]] = List(
    Class.forName("org.hsqldb.jdbc.JDBCDriver")
  )
}
