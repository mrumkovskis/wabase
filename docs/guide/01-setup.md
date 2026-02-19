# Part 1: Project Setup

In this tutorial, we build a **Task Management System** (TMS) using the same runtime conventions used in this branch (`tables/`, `views/`, `routes/` metadata folders, `doAction` route handler alias, and top-level `port` config).

## Prerequisites

*   **Java**: JDK 11 (required by this branch).
*   **sbt**: 1.11.7+.
*   **Database**: PostgreSQL (recommended).

## 1. Create Project

```bash
mkdir wabase-tms
cd wabase-tms
mkdir -p src/main/resources/{tables,views,routes,jobs}
```

## 2. Configure `build.sbt`

Create `build.sbt`:

```scala
name := "wabase-tms"
version := "0.1.0"
scalaVersion := "2.13.18"

val wabaseVersion = "8.0.0-RC37-SNAPSHOT" // current branch line

resolvers += "snapshots" at "https://central.sonatype.com/repository/maven-snapshots/"

javacOptions ++= Seq("-source", "11", "-target", "11", "-Xlint")

libraryDependencies ++= Seq(
  "org.wabase"             %% "wabase"                 % wabaseVersion,
  "org.postgresql"         %  "postgresql"             % "42.7.9",
  "ch.qos.logback"         %  "logback-classic"        % "1.5.25",
  "io.github.samueleresca" %% "pekko-quartz-scheduler" % "1.3.0-pekko-1.1.x",
  "org.scalatest"          %% "scalatest"              % "3.2.19" % Test
)

Compile / mainClass := Some("org.wabase.WabaseServer")
```

If you are targeting a stable release instead of this branch snapshot, switch `wabaseVersion` to the published release and remove the snapshots resolver.

## 3. Configure `application.conf`

Create `src/main/resources/application.conf`:

```hocon
jdbc.cp {
  main {
    jdbcUrl = "jdbc:postgresql://localhost:5432/tms_db"
    username = "tms_user"
    password = "secret_password"
    maximumPoolSize = 10
  }
}

app {
  host = "http://localhost:8080"
  files.path = "./data/files"
  auth.crypto.key = "01234567890123456789012345678901"
  auth.mac.key    = "01234567890123456789012345678901"
  job.actor       = org.wabase.WabaseJobActor
}

app.server.bind-address = "0.0.0.0"
port = 8080
```

## 4. Run

```bash
sbt run
```

If startup is successful, the server binds to `http://localhost:8080`.

### Optional: Custom Main Class

```scala
package com.example.tms

import org.wabase.WabaseServer

object TMSApp extends App {
  WabaseServer()
}
```

Then set:

```scala
Compile / mainClass := Some("com.example.tms.TMSApp")
```

**Next Step:** [Data Model and Basic CRUD](02-basic-crud.md)
