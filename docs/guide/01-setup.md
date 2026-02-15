# Part 1: Project Setup

In this tutorial, we will build a **Task Management System** (TMS). We will use the powerful code-generation features of Wabase to speed up development.

## Prerequisites

*   **Java**: JDK 11 or higher (JDK 21 recommended).
*   **sbt**: Scala Build Tool (version 1.9.0+ recommended).
*   **Database**: PostgreSQL (recommended) or HSQLDB.

## Setup Strategy

We recommend the **Manual Setup** to ensure all dependencies are compatible with the current Wabase version. There is a Giter8 template available (`guntiso/wabase-template.g8`), but it may contain experimental configurations (e.g., Java 25 requirements) that are not suitable for a standard start.

## Manual Setup

### 1. Create the sbt Project

Create a directory `wabase-tms`.

### 2. Configure Plugins

Create `project/plugins.sbt`. We will use **Mojoz** for code generation and **sbt-assembly** for deployment.

```scala
addSbtPlugin("org.mojoz" % "sbt-mojoz" % "0.1.0") // Check for latest version
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.5")
```

### 3. Configure Build (`build.sbt`)

Create `build.sbt`:

```scala
name := "wabase-tms"

version := "0.1.0"

scalaVersion := "2.13.12"

val wabaseVersion = "6.0.2"

// Enable Mojoz plugins for code generation
lazy val root = (project in file("."))
  .enablePlugins(MojozPlugin, MojozGenerateSchemaPlugin)
  .settings(
    libraryDependencies ++= Seq(
      "org.wabase"             %% "wabase"                 % wabaseVersion,
      "org.postgresql"         %  "postgresql"             % "42.6.0",
      "ch.qos.logback"         %  "logback-classic"        % "1.4.7",
      "io.github.samueleresca" %% "pekko-quartz-scheduler" % "1.1.0-pekko-1.0.x", // For jobs
      "org.scalatest"          %% "scalatest"              % "3.2.15" % Test
    ),
    // Mojoz Settings
    mojozDtosPackage := "com.example.tms.dto",
    mojozDtosImports := Seq("org.tresql._", "org.wabase.{ Dto, DtoWithId }"),
    // Generate SQL schema to db/db-schema.sql
    mojozSchemaSqlFiles := Seq((baseDirectory.value / "db" / "db-schema.sql")),
    mojozSchemaSqlGenerators := Seq(
      org.mojoz.metadata.out.DdlGenerator.postgresql(typeDefs = mojozTypeDefs.value)
    ),
    // Run the Wabase server directly
    Compile / mainClass := Some("org.wabase.WabaseServer")
  )
```

## 4. Configure Application

Create `src/main/resources/application.conf`.

```hocon
# Database Connection
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

  # Security (Replace with random strings in production!)
  auth.crypto.key = "01234567890123456789012345678901"
  auth.mac.key    = "01234567890123456789012345678901"

  # Enable jobs
  job.actor = org.wabase.WabaseJobActor
}

app.server {
  bind-address = "0.0.0.0"
  port = 8080
}
```

## 5. Running the Application

Since we defined `mainClass` in `build.sbt`, you can simply run:

```bash
sbt run
```

### Custom Startup (Optional)

If you need to customize the startup process (e.g., adding custom actors), create `src/main/scala/TMSApp.scala`:

```scala
package com.example.tms

import org.wabase.WabaseServer

object TMSApp extends App {
  // Initialize and start the Wabase server
  WabaseServer()
}
```

And update `build.sbt`:
```scala
Compile / mainClass := Some("com.example.tms.TMSApp")
```

**Next Step:** [Defining Data Model & Basic CRUD](02-basic-crud.md)
