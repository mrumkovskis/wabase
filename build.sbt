val scalaV    = "2.13.18" // "3.3.7"

val pekkoV    = "1.4.0"
val pekkoConnV= "1.2.0"
val pekkoHttpV= "1.3.0"

val mojozV    = "7.0.3"
val quereaseV = "9.1.0"
val tresqlV   = "13.3.0"

javacOptions ++= Seq("-source", "11", "-target", "11", "-Xlint")
initialize := {
  val _ = initialize.value
  val javaVersion = sys.props("java.specification.version")
  if (javaVersion != "11")
    sys.error("Java 11 is required for this project. Found " + javaVersion + " instead")
}

ThisBuild / versionScheme          := Some("semver-spec")
ThisBuild / versionPolicyIntention := Compatibility.BinaryCompatible

lazy val commonSettings = Seq(
  organization := "org.wabase",
  name := "wabase",
  scalaVersion := scalaV,
  crossScalaVersions := Seq(
    "3.3.7",
    "2.13.18",
    "2.12.21",
  ),
  scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature"),
  resolvers += "snapshots" at "https://central.sonatype.com/repository/maven-snapshots/",
  libraryDependencies ++= {
    val borerV    = scalaVersion.value match {
      case v if v startsWith "2.12" => "1.7.2"
      case v if v startsWith "2.13" => "1.8.0"
      case v if v startsWith "3"    => "1.16.2"
    }
    (
      if (scalaVersion.value.startsWith("3."))
        Seq(
          "io.bullet"              %% "borer-compat-pekko"    % borerV,
        )
      else Nil
    ) ++
    Seq(
      "com.samskivert"              % "jmustache"             % "1.16",
      "org.apache.pekko"           %% "pekko-actor"           % pekkoV,
      "org.apache.pekko"           %% "pekko-actor-typed"     % pekkoV,
      "org.apache.pekko"           %% "pekko-http"            % pekkoHttpV,
      "org.apache.pekko"           %% "pekko-connectors-csv"  % pekkoConnV, //      % Optional?
      "org.apache.pekko"           %% "pekko-connectors-xml"  % pekkoConnV, //      % Optional?
      "org.apache.pekko"           %% "pekko-http-spray-json" % pekkoHttpV,
      "org.apache.pekko"           %% "pekko-slf4j"           % pekkoV,
      "org.apache.pekko"           %% "pekko-stream"          % pekkoV,
      "com.typesafe.scala-logging" %% "scala-logging"         % "3.9.5",
      "com.typesafe"               %% "ssl-config-core"       % "0.6.1",
      "com.zaxxer"                  % "HikariCP"              % "7.0.2",
      "ch.qos.logback"              % "logback-classic"       % "1.5.25",
      "org.mojoz"                  %% "mojoz"                 % mojozV,
      "org.mojoz"                  %% "querease"              % quereaseV,
      "commons-validator"           % "commons-validator"     % "1.10.1",
      "org.postgresql"              % "postgresql"            % "42.7.9",
      "com.lambdaworks"             % "scrypt"                % "1.4.0",
      "org.tresql"                 %% "tresql"                % tresqlV,
      "io.bullet"                  %% "borer-core"            % borerV,
      "io.bullet"                  %% "borer-derivation"      % borerV,
      "org.bouncycastle"            % "bcprov-jdk18on"        % "1.83"              % Optional,
      "org.bouncycastle"            % "bcpkix-jdk18on"        % "1.83"              % Optional,
      "com.github.jwt-scala"       %% "jwt-core"              % "11.0.3"            % Optional,
      "com.github.jwt-scala"       %% "jwt-json-common"       % "11.0.3"            % Optional,
      "io.github.samueleresca"     %% "pekko-quartz-scheduler"% "1.3.0-pekko-1.1.x" % Optional,
      "io.swagger.core.v3"          % "swagger-jaxrs2-jakarta"% "2.2.45"            % Optional,
      "org.xhtmlrenderer"           % "flying-saucer-pdf"     % "9.5.2"             % Optional,
      "org.simplejavamail"          % "simple-java-mail"      % "8.12.6"            % Optional,
      "org.graalvm.js"              % "js"                    % "22.3.5"            % Optional,
      "org.graalvm.js"              % "js-scriptengine"       % "22.3.5"            % Optional,
    ) ++ Seq( // for test
      "org.scalatest"              %% "scalatest"             % "3.2.20"  %     Test,
      "org.apache.pekko"           %% "pekko-http-testkit"    % pekkoHttpV%     Test,
      "org.apache.pekko"           %% "pekko-testkit"         % pekkoV    %     Test,
      "org.apache.pekko"           %% "pekko-stream-testkit"  % pekkoV    %     Test,
      "org.hsqldb"                  % "hsqldb"                % "2.7.4"   %     Test,
      "com.vladsch.flexmark"        % "flexmark-all"          % "0.64.8"  %     Test,
    )
  },
)

lazy val wabase = (project in file("."))
  .settings(commonSettings: _*)
  .settings(
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
    Compile / unmanagedSourceDirectories ++= {
      val sharedSourceDir = (ThisBuild / baseDirectory).value / "compat"
      if (scalaVersion.value.startsWith("2."))
        Seq(sharedSourceDir / "scala-2")
      else Nil
    },
  )
  .settings(
    Compile / doc / scalacOptions ++= (baseDirectory map { bd =>
      Seq("-sourcepath", bd.getAbsolutePath,
        "-doc-source-url", "https://github.com/mrumkovskis/wabase/blob/develop€{FILE_PATH}.scala")
    }).value)
  .settings(
    publishTo := {
      val centralSnapshots = "https://central.sonatype.com/repository/maven-snapshots/"
      if (isSnapshot.value)
        Some("central-snapshots" at centralSnapshots)
      else
        localStaging.value
    },
    publishMavenStyle := true,
    Test / publishArtifact := true,
    Test / packageBin / mappings ~= { _.filter { m =>
      !m._1.getName.endsWith(".conf")       &&
      !m._1.getName.endsWith(".properties") &&
      !m._1.getName.endsWith(".xml")        &&
      !m._2.startsWith("routes")            &&
      !m._2.startsWith("tables")            &&
      !m._2.startsWith("views")
    }}
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

lazy val it = (project in file("src/it"))
  .dependsOn(wabase)
  .settings(commonSettings: _*)
  .settings(
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19",
    publish / skip := true,
    Compile / resourceDirectory := baseDirectory.value / "resources",
    Compile / run / mainClass   := Some("org.wabase.WabaseServer"),
    Compile / unmanagedSources  += baseDirectory.value / ".." / "test" / "scala" / "BusinessScenariosBaseSpecs.scala",
    Compile / unmanagedSources  += baseDirectory.value / ".." / "test" / "scala" / "TemplateUtil.scala",
    Test / fork := true,
    Test / javaOptions := Seq("-Xmx2G"),
    Test / parallelExecution := false,
    Test / resourceDirectory := baseDirectory.value / "resources",
    Test / scalaSource       := baseDirectory.value / "scala",
    Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-h", name.value + "-it-report"),
  )

lazy val it_legacy = (project in file("src/it_legacy"))
  .dependsOn(wabase)
  .settings(commonSettings: _*)
  .settings(
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19",
    publish / skip := true,
    Compile / resourceDirectory := baseDirectory.value / "resources",
    Compile / run / mainClass   := Some("org.wabase.WabaseServer"),
    Compile / unmanagedSources  += baseDirectory.value / ".." / "test" / "scala" / "BusinessScenariosBaseSpecs.scala",
    Compile / unmanagedSources  += baseDirectory.value / ".." / "test" / "scala" / "TemplateUtil.scala",
    Test / fork := true,
    Test / javaOptions := Seq("-Xmx2G"),
    Test / parallelExecution := false,
    Test / resourceDirectory := baseDirectory.value / "resources",
    Test / scalaSource       := baseDirectory.value / "scala",
    Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-h", name.value + "-it-report"),
  )

Test            / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-h", "report")

Test            / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oDSF")
