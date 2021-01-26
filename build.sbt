val scalaV = "2.13.4"

val akkaHttpV = "10.2.2"
val akkaV = "2.6.11"

lazy val dependencies = {
  Seq(
    "com.typesafe.akka"          %% "akka-actor"                        % akkaV,
    "com.typesafe.akka"          %% "akka-http-spray-json"              % akkaHttpV,
    "com.typesafe.akka"          %% "akka-http-xml"                     % akkaHttpV,
    "com.typesafe.akka"          %% "akka-slf4j"                        % akkaV,
    "com.typesafe.akka"          %% "akka-stream"                       % akkaV,
    "com.typesafe.scala-logging" %% "scala-logging"                     % "3.9.2",
    "com.zaxxer"                  % "HikariCP"                          % "3.4.5",
    "ch.qos.logback"              % "logback-classic"                   % "1.2.3",
    "org.tresql"                 %% "tresql"                            % "10.1.1",
    "org.mojoz"                  %% "querease"                          % "6.0.0-SNAPSHOT",
    "commons-validator"           % "commons-validator"                 % "1.7",
    "commons-codec"               % "commons-codec"                     % "1.15",
    "org.postgresql"              % "postgresql"                        % "42.2.18",
    "com.lambdaworks"             % "scrypt"                            % "1.4.0",
  )
}

lazy val testDependencies = Seq(
    "org.scalatest"              %% "scalatest"                         % "3.2.2" % "it,test",
    "com.typesafe.akka"          %% "akka-http-testkit"                 % akkaHttpV % "it,test",
    "com.typesafe.akka"          %% "akka-testkit"                      % akkaV   % "it,test",
    "com.typesafe.akka"          %% "akka-stream-testkit"               % akkaV   % "it,test",
    "org.hsqldb"                  % "hsqldb"                            % "2.5.1" %    "test",
    "com.vladsch.flexmark"        % "flexmark-all"                      % "0.35.10" % "it,test",
)

ThisBuild / sbt.Keys.versionScheme := Some("semver-spec")

lazy val wabase = (project in file("."))
  .configs(IntegrationTest extend(Test))
  .settings(Defaults.itSettings : _*)
  .settings(
  organization := "org.wabase",
  name := "wabase",
  scalaVersion := scalaV,
  crossScalaVersions := Seq(
    scalaV,
    "2.12.13",
  ),
  scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature",
    "-Xmacro-settings:metadataFactoryClass=org.mojoz.querease.TresqlMetadataFactory" +
      ", tableMetadataFile=" + (baseDirectory.value / "tresql-table-metadata.yaml").getAbsolutePath),
  resolvers += "snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  libraryDependencies ++= dependencies ++ testDependencies,
  apiMappings ++= (fullClasspath in Compile map { fcp =>
    // fix bad api mappings,
    val mappings: Map[String, String] =
      fcp.files.map(_.getName).filter(_ startsWith "akka-").filterNot(_ startsWith "akka-http-")
        .map(akkajar => (akkajar, s"http://doc.akka.io/api/akka/$akkaV/")).toMap ++
      fcp.files.map(_.getName).filter(_ startsWith "akka-http-")
        .map(akkajar => (akkajar, s"http://doc.akka.io/api/akka/$akkaHttpV/")).toMap
    fcp.files.filter(f => mappings.contains(f.getName))
      .map(f => (f, new java.net.URL(mappings(f.getName)))).toMap
  }).value,
  updateOptions := updateOptions.value.withLatestSnapshots(false),
  )
  /*
  .settings(
    initialCommands in console := s"""
      |import akka.actor._
      |import akka.stream._
      |import scaladsl._
      |import stage._
      |import Attributes._
      |import akka.http._
      |import scala.concurrent._
      |import duration._
      |import akka.http.scaladsl.model._
      |import akka.http.scaladsl.server._
      |import Directives._
      |import akka.http.scaladsl.client.RequestBuilding._
      |//import akka.http.scaladsl.testkit._
      |//import org.scalatest.{FlatSpec, Matchers, WordSpec}
      |import org.wabase._
      |//implicit val system = ActorSystem("test-system") //creates problems with scalatest call from test:console
      |//implicit val materializer = ActorMaterializer()
      |//implicit val executionContext = system.dispatcher""".stripMargin
)
*/
  .settings(
    Compile / unmanagedSourceDirectories ++= {
      val sharedSourceDir = (ThisBuild / baseDirectory).value / "compat"
      if (scalaVersion.value.startsWith("2.12."))
        Seq(sharedSourceDir / "scala-2.12")
      else Nil
    },
  )
  .settings(
    scalacOptions in (Compile, doc) ++= (baseDirectory map { bd =>
      Seq("-sourcepath", bd.getAbsolutePath,
        "-doc-source-url", "https://github.com/mrumkovskis/wabase/blob/develop€{FILE_PATH}.scala")
    }).value)
  .settings(
    publishTo := version { v: String =>
      val nexus = "https://oss.sonatype.org/"
      if (v.trim.endsWith("SNAPSHOT"))
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases" at nexus + "service/local/staging/deploy/maven2")
    }.value,
    publishMavenStyle := true,
    publishArtifact in Test := true,
    //publishArtifact in IntegrationTest := true, --does not work, https://github.com/sbt/sbt/issues/2458
    addArtifact(artifact in (IntegrationTest, packageBin), packageBin in IntegrationTest),
    addArtifact(artifact in (IntegrationTest, packageDoc), packageDoc in IntegrationTest),
    addArtifact(artifact in (IntegrationTest, packageSrc), packageSrc in IntegrationTest)
  )
  .settings(
    pomIncludeRepository := { _ => false },
    pomExtra := <url>https://github.com/mrumkovskis/wabase</url>
      <licenses>
        <license>
          <name>MIT</name>
          <url>http://www.opensource.org/licenses/MIT</url>
          <distribution>repo</distribution>
        </license>
      </licenses>
      <scm>
        <url>git@github.com:mrumkovskis/wabase.git</url>
        <connection>scm:git:git@github.com:mrumkovskis/wabase.git</connection>
      </scm>
      <developers>
        <developer>
          <id>mrumkovskis</id>
          <name>Martins Rumkovskis</name>
          <url>https://github.com/mrumkovskis/</url>
        </developer>
        <developer>
          <id>guntiso</id>
          <name>Guntis Ozols</name>
          <url>https://github.com/guntiso/</url>
        </developer>
        <developer>
          <id>muntis</id>
          <name>Muntis Grube</name>
          <url>https://github.com/muntis/</url>
        </developer>
        <developer>
          <id>janqis</id>
          <name>Janis Birgelis</name>
          <url>https://github.com/janqis/</url>
        </developer>
      </developers>
  )

fork in IntegrationTest in ThisBuild := true
javaOptions in IntegrationTest in ThisBuild := Seq("-Xmx1G")

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-h", "report")

testOptions in IntegrationTest += Tests.Argument(TestFrameworks.ScalaTest, "-h", "it-report")

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oDS")

testOptions in IntegrationTest += Tests.Argument(TestFrameworks.ScalaTest, "-oDS")
