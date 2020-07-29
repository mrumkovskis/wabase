libraryDependencies += "org.mojoz" %% "querease" % "1.2.2-SNAPSHOT"

resolvers += "snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

sourceDirectory := baseDirectory(_ / ".." / "src").value

unmanagedSources in Compile ~= { _ filter(_.getName == "AppMetadata.scala") }
