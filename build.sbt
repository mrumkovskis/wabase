val scalaV    = "2.13.18" // "3.3.7"

val pekkoV    = "1.7.0"
val pekkoConnV= "1.3.0"
val pekkoHttpV= "1.4.0"

// In-process SMTP server for email tests. Exclusions - jakarta mail api and implementation
// are already provided by simple-java-mail (as jakarta.mail-api and angus-mail).
val greenmailDependency = ("com.icegreen" % "greenmail" % "2.1.3")
  .exclude("org.eclipse.angus", "jakarta.mail")
  .exclude("junit", "junit")

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
    "3.3.8",
    "2.13.18",
    "2.12.21",
  ),
  scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature"),
  resolvers += "snapshots" at "https://central.sonatype.com/repository/maven-snapshots/",
  libraryDependencies ++= {
    val borerV    = scalaVersion.value match {
      case v if v startsWith "2.12" => "1.7.2"
      case v if v startsWith "2.13" => "1.8.0"
      case v if v startsWith "3"    => "1.17.0"
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
      "org.apache.pekko"           %% "pekko-connectors-json-streaming" % pekkoConnV,
      "org.apache.pekko"           %% "pekko-http-spray-json" % pekkoHttpV,
      "org.apache.pekko"           %% "pekko-slf4j"           % pekkoV,
      "org.apache.pekko"           %% "pekko-stream"          % pekkoV,
      "com.typesafe.scala-logging" %% "scala-logging"         % "3.9.6",
      "com.typesafe"               %% "ssl-config-core"       % "0.7.1",
      "com.zaxxer"                  % "HikariCP"              % "7.1.0",
      "ch.qos.logback"              % "logback-classic"       % "1.6.3",
      "org.mojoz"                  %% "mojoz"                 % "7.2.1",
      "org.mojoz"                  %% "querease"              % "10.2.1",
      "commons-validator"           % "commons-validator"     % "1.11.0",
      "org.postgresql"              % "postgresql"            % "42.7.13",
      "com.lambdaworks"             % "scrypt"                % "1.4.0",
      "org.tresql"                 %% "tresql"                % "13.5.1",
      "io.bullet"                  %% "borer-core"            % borerV,
      "io.bullet"                  %% "borer-derivation"      % borerV,
      "org.bouncycastle"            % "bcprov-jdk18on"        % "1.85.2"            % Optional,
      "org.bouncycastle"            % "bcpkix-jdk18on"        % "1.85"              % Optional,
      "com.github.jwt-scala"       %% "jwt-core"              % "11.0.4"            % Optional,
      "com.github.jwt-scala"       %% "jwt-json-common"       % "11.0.4"            % Optional,
      "io.github.samueleresca"     %% "pekko-quartz-scheduler"% "1.3.0-pekko-1.1.x" % Optional,
      "io.swagger.core.v3"          % "swagger-jaxrs2-jakarta"% "2.2.54"            % Optional,
      "org.xhtmlrenderer"           % "flying-saucer-pdf"     % "9.5.2"             % Optional,
      "org.simplejavamail"          % "simple-java-mail"      % "9.3.2"             % Optional,
      "org.graalvm.js"              % "js"                    % "22.3.5"            % Optional,
      "org.graalvm.js"              % "js-scriptengine"       % "22.3.5"            % Optional,
    ) ++ Seq( // for test
      "org.scalatest"              %% "scalatest"             % "3.2.20"  %     Test,
      "org.apache.pekko"           %% "pekko-http-testkit"    % pekkoHttpV%     Test,
      "org.apache.pekko"           %% "pekko-testkit"         % pekkoV    %     Test,
      "org.apache.pekko"           %% "pekko-stream-testkit"  % pekkoV    %     Test,
      "org.hsqldb"                  % "hsqldb"                % "2.7.4"   %     Test,
      "com.vladsch.flexmark"        % "flexmark-all"          % "0.64.8"  %     Test,
      greenmailDependency                                                 %     Test,
    )
  },
  // Binary compatibility exceptions against previous release.
  // Request decoder configuration is consolidated under 'request-decoders' conf and per format decoder
  // configs and factory traits are replaced by single StreamDecoderFactory trait, factory per parser.
  mimaBinaryIssueFilters ++= {
    import com.typesafe.tools.mima.core._
    Seq(
      ProblemFilters.exclude[MissingClassProblem]("org.wabase.CsvDecoderConfig"),
      ProblemFilters.exclude[MissingClassProblem]("org.wabase.CsvDecoderConfig$"),
      ProblemFilters.exclude[MissingClassProblem]("org.wabase.JsonDecoderConfig"),
      ProblemFilters.exclude[MissingClassProblem]("org.wabase.JsonDecoderConfig$"),
      ProblemFilters.exclude[MissingClassProblem]("org.wabase.XmlDecoderConfig"),
      ProblemFilters.exclude[MissingClassProblem]("org.wabase.XmlDecoderConfig$"),
      ProblemFilters.exclude[IncompatibleTemplateDefProblem]("org.wabase.CsvDecoderFactory"),
      ProblemFilters.exclude[IncompatibleTemplateDefProblem]("org.wabase.JsonDecoderFactory"),
      ProblemFilters.exclude[IncompatibleTemplateDefProblem]("org.wabase.XmlDecoderFactory"),
      ProblemFilters.exclude[MissingTypesProblem]("org.wabase.CsvDecoderFactory$"),
      ProblemFilters.exclude[MissingTypesProblem]("org.wabase.JsonDecoderFactory$"),
      ProblemFilters.exclude[MissingTypesProblem]("org.wabase.XmlDecoderFactory$"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.wabase.CsvDecoderFactory.createCsvStreamDecoder"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.wabase.CsvDecoderFactory.createCsvStreamDecoders"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.wabase.JsonDecoderFactory.createJsonStreamDecoder"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.wabase.JsonDecoderFactory.createJsonStreamDecoders"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.wabase.XmlDecoderFactory.createXmlStreamDecoder"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.wabase.XmlDecoderFactory.createXmlStreamDecoders"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.wabase.RequestDecoders.<clinit>"),
      // Job is identified by name instead of view definition - view def is resolved by job executor
      // (see app.job.executor conf parameter) at job execution time.
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.wabase.WabaseScheduler.doJob"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.wabase.WabaseScheduler#Tick.job"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.wabase.WabaseScheduler#Tick.copy"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.wabase.WabaseScheduler#Tick.copy$default$1"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.wabase.WabaseScheduler#Tick.this"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.wabase.WabaseScheduler#Tick.apply"),
      ProblemFilters.exclude[IncompatibleSignatureProblem]("org.wabase.WabaseScheduler#Tick.unapply"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.wabase.WabaseScheduler#Tick._1"), // scala 3
      // Email action op has new 'html' option - body is sent as html instead of plain text.
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.wabase.AppMetadata#Action#Email.this"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.wabase.AppMetadata#Action#Email.apply"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.wabase.AppMetadata#Action#Email.copy"),
      ProblemFilters.exclude[MissingTypesProblem]("org.wabase.AppMetadata$Action$Email$"),
      ProblemFilters.exclude[IncompatibleSignatureProblem]("org.wabase.AppMetadata#Action#Email.unapply"),
      // Email action op attachments are wrapped in Email.Attachment to carry 'embedded' option -
      // attachment is embedded into html body as image instead of being added as regular attachment.
      ProblemFilters.exclude[IncompatibleSignatureProblem]("org.wabase.AppMetadata#Action#Email.attachmentsOp"),
      ProblemFilters.exclude[IncompatibleSignatureProblem]("org.wabase.AppMetadata#Action#Email.copy$default$4"),
      ProblemFilters.exclude[IncompatibleSignatureProblem]("org.wabase.AppMetadata#Action#Email.<init>$default$4"),
      ProblemFilters.exclude[IncompatibleSignatureProblem]("org.wabase.AppMetadata#Action#Email.apply$default$4"),
      ProblemFilters.exclude[IncompatibleSignatureProblem]("org.wabase.AppMetadata#Action#Email._4"), // scala 3
      // Extract entity action op source is specified in 'from' clause, field 'op' renamed to 'source'.
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.wabase.AppMetadata#Action#ExtractHttpEntity.op"),
    )
  },
)

lazy val wabase = (project in file("."))
  .settings(commonSettings: _*)
  .settings(
    // Explicit Scaladoc base URLs for Apache Pekko jars (sbt-api-mappings does not
    // cover these). Match by Maven path so third-party "pekko-*" artifacts are skipped.
    apiMappings ++= {
      val jars = (Compile / fullClasspath).value.files
        .filter(_.getPath.replace('\\', '/').contains("/org/apache/pekko/"))
      def baseUrl(jarName: String): Option[String] =
        if (jarName.startsWith("pekko-http") || jarName.startsWith("pekko-parsing"))
          Some(s"https://pekko.apache.org/api/pekko-http/$pekkoHttpV/")
        else if (jarName.startsWith("pekko-connectors"))
          Some(s"https://pekko.apache.org/api/pekko-connectors/$pekkoConnV/")
        else
          Some(s"https://pekko.apache.org/api/pekko/$pekkoV/")
      jars.flatMap(j => baseUrl(j.getName).map(u => j -> url(u))).toMap
    },
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

// Shared by it / it_legacy: resources dir is used for both Compile and Test, but
// logback-test.xml must only appear once on the test classpath (Logback warns if it
// is also copied to Compile/classes).
lazy val itResourceSettings = Seq(
  Compile / resourceDirectory := baseDirectory.value / "resources",
  // Keep app config/routes/views on compile classpath; exclude test-only Logback config.
  Compile / unmanagedResources / excludeFilter := {
    (Compile / unmanagedResources / excludeFilter).value ||
      new SimpleFileFilter(_.getName == "logback-test.xml")
  },
  Test / resourceDirectory := baseDirectory.value / "resources",
  // Test must not inherit Compile's logback-test.xml exclusion (scope delegation).
  Test / unmanagedResources / excludeFilter := HiddenFileFilter,
  Test / scalaSource       := baseDirectory.value / "scala",
  Test / fork := true,
  Test / javaOptions := Seq("-Xmx2G"),
  Test / parallelExecution := false,
  Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-h", name.value + "-it-report"),
)

lazy val it = (project in file("src/it"))
  .dependsOn(wabase)
  .settings(commonSettings: _*)
  .settings(itResourceSettings: _*)
  .settings(
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.20",
    libraryDependencies += greenmailDependency,
    publish / skip := true,
    Compile / run / mainClass   := Some("org.wabase.WabaseServer"),
    Compile / unmanagedSources  += baseDirectory.value / ".." / "test" / "scala" / "BusinessScenariosBaseSpecs.scala",
    Compile / unmanagedSources  += baseDirectory.value / ".." / "test" / "scala" / "GreenMailServer.scala",
    Compile / unmanagedSources  += baseDirectory.value / ".." / "test" / "scala" / "TemplateUtil.scala",
  )

lazy val it_legacy = (project in file("src/it_legacy"))
  .dependsOn(wabase)
  .settings(commonSettings: _*)
  .settings(itResourceSettings: _*)
  .settings(
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.20",
    libraryDependencies += greenmailDependency,
    publish / skip := true,
    Compile / run / mainClass   := Some("org.wabase.WabaseServer"),
    Compile / unmanagedSources  += baseDirectory.value / ".." / "test" / "scala" / "BusinessScenariosBaseSpecs.scala",
    Compile / unmanagedSources  += baseDirectory.value / ".." / "test" / "scala" / "GreenMailServer.scala",
    Compile / unmanagedSources  += baseDirectory.value / ".." / "test" / "scala" / "TemplateUtil.scala",
  )

Test            / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-h", "report")

Test            / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oDSF")
