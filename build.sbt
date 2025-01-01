val scalaV    = "3.3.4"

val pekkoV    = "1.1.2"
val pekkoHttpV= "1.1.0"

val mojozV    = "5.3.3"
val quereaseV = "7.0.1"
val tresqlV   = "12.0.1"

javacOptions ++= Seq("-source", "11", "-target", "11", "-Xlint")
initialize := {
  val _ = initialize.value
  val javaVersion = sys.props("java.specification.version")
  if (javaVersion != "11")
    sys.error("Java 11 is required for this project. Found " + javaVersion + " instead")
}

ThisBuild / versionScheme          := Some("semver-spec")
ThisBuild / versionPolicyIntention := Compatibility.BinaryCompatible

lazy val wabase = (project in file("."))
  .settings(
  organization := "org.wabase",
  name := "wabase",
  scalaVersion := scalaV,
  crossScalaVersions := Seq(
    "3.3.4",
    // "2.13.15",
    // "2.12.20",
  ),
  scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature"),
  resolvers += "snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  libraryDependencies ++= {
    val borerV    = scalaVersion.value match {
      case v if v startsWith "2.12" => "1.7.2"
      case v if v startsWith "2.13" => "1.8.0"
      case v if v startsWith "3"    => "1.15.0"
    }
    Seq(
      "com.samskivert"              % "jmustache"             % "1.16",
      "org.apache.pekko"           %% "pekko-actor"           % pekkoV,
      "org.apache.pekko"           %% "pekko-http-spray-json" % pekkoHttpV,
      "org.apache.pekko"           %% "pekko-slf4j"           % pekkoV,
      "org.apache.pekko"           %% "pekko-stream"          % pekkoV,
      "com.typesafe.scala-logging" %% "scala-logging"         % "3.9.5",
      "com.typesafe"               %% "ssl-config-core"       % "0.6.1",
      "com.zaxxer"                  % "HikariCP"              % "6.2.1",
      "ch.qos.logback"              % "logback-classic"       % "1.5.15",
      "org.mojoz"                  %% "mojoz"                 % mojozV,
      "org.mojoz"                  %% "querease"              % quereaseV,
      "commons-validator"           % "commons-validator"     % "1.9.0",
      "org.postgresql"              % "postgresql"            % "42.7.4",
      "com.lambdaworks"             % "scrypt"                % "1.4.0",
      "org.tresql"                 %% "tresql"                % tresqlV,
      "io.bullet"                  %% "borer-core"            % borerV,
      "io.bullet"                  %% "borer-derivation"      % borerV,
      "io.bullet"                  %% "borer-compat-pekko"    % borerV,
      "io.github.samueleresca"     %% "pekko-quartz-scheduler"% "1.3.0-pekko-1.1.x" % Optional,
      "org.xhtmlrenderer"           % "flying-saucer-pdf"     % "9.11.2"            % Optional,
      "org.simplejavamail"          % "simple-java-mail"      % "8.12.4"            % Optional,
      "org.graalvm.js"              % "js"                    % "22.3.5"            % Optional,
      "org.graalvm.js"              % "js-scriptengine"       % "22.3.5"            % Optional,
    ) ++ Seq( // for test
      "org.scalatest"              %% "scalatest"             % "3.2.19"  %     Test,
      "org.apache.pekko"           %% "pekko-http-testkit"    % pekkoHttpV%     Test,
      "org.apache.pekko"           %% "pekko-testkit"         % pekkoV    %     Test,
      "org.apache.pekko"           %% "pekko-stream-testkit"  % pekkoV    %     Test,
      "org.hsqldb"                  % "hsqldb"                % "2.7.4"   %     Test,
      "com.vladsch.flexmark"        % "flexmark-all"          % "0.64.8"  %     Test,
    )
  },
  /*
  apiMappings ++= (Compile / fullClasspath map { fcp =>
    // fix bad api mappings,
    val mappings: Map[String, String] =
      fcp.files.map(_.getName).filter(_ startsWith "pekko-").filterNot(_ startsWith "pekko-http-")
        .map(akkajar => (akkajar, s"http://doc.pekko.io/api/pekko/$pekkoV/")).toMap ++
      fcp.files.map(_.getName).filter(_ startsWith "pekko-http-")
        .map(akkajar => (akkajar, s"http://doc.pekko.io/api/pekko/$pekkoHttpV/")).toMap
    fcp.files.filter(f => mappings.contains(f.getName))
      .map(f => (f, new java.net.URL(mappings(f.getName)))).toMap
  }).value,
  */
  updateOptions := updateOptions.value.withLatestSnapshots(false),
  )
  /*
  .settings(
    initialCommands in console := s"""
      |import org.apache.pekko.actor._
      |import org.apache.pekko.stream._
      |import scaladsl._
      |import stage._
      |import Attributes._
      |import org.apache.pekko.http._
      |import scala.concurrent._
      |import duration._
      |import org.apache.pekko.http.scaladsl.model._
      |import org.apache.pekko.http.scaladsl.server._
      |import Directives._
      |import org.apache.pekko.http.scaladsl.client.RequestBuilding._
      |//import org.apache.pekko.http.scaladsl.testkit._
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
    Test / unmanagedSourceDirectories += baseDirectory.value / "src" / "it" / "scala",
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
    Test / packageBin / mappings ~= { _.filter(m =>
      !m._1.getName.endsWith(".conf")       &&
      !m._1.getName.endsWith(".properties") &&
      !m._1.getName.endsWith(".xml")
    )}
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

Test            / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-h", "report")

Test            / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oDSF")
