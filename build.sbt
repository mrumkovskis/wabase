val scalaV = "2.13.3"

val akkaHttpV = "10.1.12"
val akkaV = "2.6.5"

lazy val dependencies = {
  Seq(
    "com.typesafe.akka"          %% "akka-actor"                        % akkaV,
    "com.typesafe.akka"          %% "akka-http-spray-json"              % akkaHttpV,
    "com.typesafe.akka"          %% "akka-http-xml"                     % akkaHttpV,
    "com.typesafe.akka"          %% "akka-slf4j"                        % akkaV,
    "com.typesafe.akka"          %% "akka-stream"                       % akkaV,
    "com.typesafe.scala-logging" %% "scala-logging"                     % "3.9.2",
    "com.zaxxer"                  % "HikariCP"                          % "3.4.2",
    "ch.qos.logback"              % "logback-classic"                   % "1.2.3",
    "org.mojoz"                  %% "querease"                          % "4.0.0-SNAPSHOT",
    "org.tresql"                 %% "tresql"                            % "10.0.0-SNAPSHOT",
    "org.mojoz"                  %% "mojoz"                             % "1.2.1",
    "commons-validator"           % "commons-validator"                 % "1.5.0",
    "commons-codec"               % "commons-codec"                     % "1.10",
    "org.postgresql"              % "postgresql"                        % "42.2.5",
    "com.lambdaworks"             % "scrypt"                            % "1.4.0",
  )
}

lazy val testDependencies = Seq(
    "org.scalatest"              %% "scalatest"                         % "3.2.0" % "it,test",
    "com.typesafe.akka"          %% "akka-http-testkit"                 % akkaHttpV % "it,test",
    "com.typesafe.akka"          %% "akka-testkit"                      % akkaV   % "it,test",
    "com.typesafe.akka"          %% "akka-stream-testkit"               % akkaV   % "it,test",
    "org.pegdown"                 % "pegdown"                           % "1.6.0" % "it,test",
    "org.hsqldb"                  % "hsqldb"                            % "2.5.0"     %    "test",
    "com.vladsch.flexmark"        % "flexmark-all"                      % "0.35.10" % "it,test",
)

lazy val commonSettings = Seq(
  name := "wabase",
  version := "0.1",
  scalaVersion := scalaV,
  crossScalaVersions := Seq(
    scalaV,
    "2.12.10",
  ),
  scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature",
    "-Xmacro-settings:metadataFactoryClass=querease.TresqlMetadataFactory" +
      ", tableMetadataFile=" + (baseDirectory.value / "tresql-table-metadata.yaml").getAbsolutePath),
  resolvers += "snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  libraryDependencies ++= dependencies ++ testDependencies,
  autoAPIMappings := true,
  apiMappings ++= (fullClasspath in Compile map { fcp =>
    // fix bad api mappings,
    // other mappings are automagical by autoAPIMappings and sbt-api-mappings plugin
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

lazy val webapp = (project in file("."))
  .settings(commonSettings: _*)
  .configs(IntegrationTest extend(Test))
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
  .settings(Defaults.itSettings : _*)
  .settings(
    scalacOptions in (Compile, doc) ++= (baseDirectory map { bd =>
      Seq("-sourcepath", bd.getAbsolutePath,
        "-doc-source-url", "https://github.com/mrumkovskis/wabase/blob/develop€{FILE_PATH}.scala")
    }).value)
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
      </developers>
  )

fork in IntegrationTest in ThisBuild := true
javaOptions in IntegrationTest in ThisBuild := Seq("-Xmx1G")

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-h", "report")

testOptions in IntegrationTest += Tests.Argument(TestFrameworks.ScalaTest, "-h", "it-report")

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oDS")

testOptions in IntegrationTest += Tests.Argument(TestFrameworks.ScalaTest, "-oDS")
