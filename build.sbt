val scalaV    = "2.13.10" // "3.2.2"

val akkaV     = "2.6.20"
val akkaHttpV = "10.2.10"

val mojozV    = "5.0.0-SNAPSHOT"
val quereaseV = "7.0.0-SNAPSHOT"
val tresqlV   = "12.0.0-SNAPSHOT"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")
initialize := {
  val _ = initialize.value
  val javaVersion = sys.props("java.specification.version")
  if (javaVersion != "1.8")
    sys.error("Java 1.8 is required for this project. Found " + javaVersion + " instead")
}

ThisBuild / versionScheme          := Some("semver-spec")
ThisBuild / versionPolicyIntention := Compatibility.BinaryCompatible

lazy val wabase = (project in file("."))
  .configs(IntegrationTest extend(Test))
  .settings(Defaults.itSettings : _*)
  .settings(
  organization := "org.wabase",
  name := "wabase",
  scalaVersion := scalaV,
  crossScalaVersions := Seq(
    "3.2.2",
    "2.13.10",
    "2.12.17",
  ),
  scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature"),
  resolvers += "snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  libraryDependencies ++= {
    val borerV    = scalaVersion.value match {
      case v if v startsWith "2.12" => "1.7.2"
      case v if v startsWith "2.13" => "1.8.0"
      case v if v startsWith "3"    => "1.10.2"
    }
    Seq(
      "com.typesafe.akka"          %% "akka-actor"            % akkaV                 cross CrossVersion.for3Use2_13,
      "com.typesafe.akka"          %% "akka-http-spray-json"  % akkaHttpV             cross CrossVersion.for3Use2_13,
      "com.typesafe.akka"          %% "akka-slf4j"            % akkaV                 cross CrossVersion.for3Use2_13,
      "com.typesafe.akka"          %% "akka-stream"           % akkaV                 cross CrossVersion.for3Use2_13,
      "com.typesafe.scala-logging" %% "scala-logging"         % "3.9.5",
      "com.typesafe"               %% "ssl-config-core"       % "0.6.1"               cross CrossVersion.for3Use2_13,
      "com.zaxxer"                  % "HikariCP"              % "4.0.3",
      "ch.qos.logback"              % "logback-classic"       % "1.3.5",
      "org.mojoz"                  %% "mojoz"                 % mojozV,
      "org.mojoz"                  %% "querease"              % quereaseV,
      "commons-validator"           % "commons-validator"     % "1.7",
      "commons-codec"               % "commons-codec"         % "1.15",
      "org.postgresql"              % "postgresql"            % "42.5.4",
      "com.lambdaworks"             % "scrypt"                % "1.4.0",
      (if (scalaVersion.value startsWith "3") {
        "org.tresql"               %% "tresql"                % tresqlV  % "provided" cross CrossVersion.for3Use2_13
       } else {
        "org.tresql"               %% "tresql"                % tresqlV
       }
      ),
      "io.bullet"                  %% "borer-core"            % borerV,
      "io.bullet"                  %% "borer-derivation"      % borerV,
      "io.bullet"                  %% "borer-compat-akka"     % borerV,
    ) ++ Seq( // for test
      "org.scalatest"              %% "scalatest"             % "3.2.15"  % "it,test",
      "com.typesafe.akka"          %% "akka-http-testkit"     % akkaHttpV % "it,test" cross CrossVersion.for3Use2_13,
      "com.typesafe.akka"          %% "akka-testkit"          % akkaV     % "it,test" cross CrossVersion.for3Use2_13,
      "com.typesafe.akka"          %% "akka-stream-testkit"   % akkaV     % "it,test" cross CrossVersion.for3Use2_13,
      "org.hsqldb"                  % "hsqldb"                % "2.7.1"   %    "test" classifier "jdk8",
      "com.vladsch.flexmark"        % "flexmark-all"          % "0.62.2"  % "it,test",
    )
  },
  apiMappings ++= (Compile / fullClasspath map { fcp =>
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
    Compile / doc / scalacOptions ++= (baseDirectory map { bd =>
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
    Test / publishArtifact := true,
    //publishArtifact in IntegrationTest := true, --does not work, https://github.com/sbt/sbt/issues/2458
    addArtifact(IntegrationTest / packageBin / artifact, IntegrationTest / packageBin),
    addArtifact(IntegrationTest / packageDoc / artifact, IntegrationTest / packageDoc),
    addArtifact(IntegrationTest / packageSrc / artifact, IntegrationTest / packageSrc)
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

ThisBuild / IntegrationTest / fork := true
ThisBuild / IntegrationTest / javaOptions := Seq("-Xmx1G")

Test            / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-h", "report")

IntegrationTest / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-h", "it-report")

Test            / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oDSF")

IntegrationTest / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oDSF")
