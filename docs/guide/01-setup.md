# Part 1: Project Setup

In this tutorial, we build a **Task Management System** (TMS) using Wabase.

## Prerequisites

*   **Java**: JDK 11 (required by the framework).
*   **sbt**: 1.11.7+.
*   **Database**: PostgreSQL (recommended).

---

## 1. Create Project (Recommended)

The fastest way to start a new Wabase project is using the Giter8 template.

```bash
sbt new guntiso/wabase-template.g8
```

This will prompt you for project name and package details, then scaffold the standard directory structure, `build.sbt`, and initial configuration.

**Next Step:** [Data Model and Basic CRUD](02-basic-crud.md)

---

## 2. Manual Setup (Advanced)

If you prefer to build the project structure manually, follow these steps.

### A. Directory Structure

```bash
mkdir wabase-tms
cd wabase-tms
mkdir -p src/main/resources/{tables,views,routes,jobs}
```

### B. Configure `build.sbt`

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

If you are targeting a stable release, switch `wabaseVersion` to the published release and remove the snapshots resolver.

### C. Configure `application.conf`

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

### D. Run

```bash
sbt run
```

If startup is successful, the server binds to `http://localhost:8080`.

**Next Step:** [Data Model and Basic CRUD](02-basic-crud.md)

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
