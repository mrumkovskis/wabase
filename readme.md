## use in your sbt project
add to your build.sbt file - libraryDependencies += "org.wabase" %% "wabase" % "1.0.0"

## To run coverage:

sbt clean coverage test coverageReport

## To run integration tests:

sbt clean it:test
