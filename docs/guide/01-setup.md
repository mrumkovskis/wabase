# Part 1: Project Setup

In this tutorial, we will build a **Task Management System** (TMS). This system will handle users, projects, and tasks. It will include file attachments, complex validations, and background jobs.

## Prerequisites

*   **Java**: JDK 11 or higher.
*   **sbt**: Scala Build Tool (version 1.9.0+ recommended).
*   **Database**: PostgreSQL (recommended) or HSQLDB (for quick testing).

## 1. Create the sbt Project

Create a new directory `wabase-tms` and add the following files.

### `build.sbt`

```scala
name := "wabase-tms"

version := "0.1.0"

scalaVersion := "2.13.12"

val wabaseVersion = "6.0.2"

libraryDependencies ++= Seq(
  "org.wabase"     %% "wabase"     % wabaseVersion,
  "org.postgresql" %  "postgresql" % "42.6.0", // Database driver
  "ch.qos.logback" %  "logback-classic" % "1.4.7" // Logging
)
```

## 2. Configure the Application

Create `src/main/resources/application.conf`. This is the brain of your Wabase application.

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

  # File storage for uploads
  files.path = "./data/files"

  # Security (Replace with random strings in production!)
  auth.crypto.key = "01234567890123456789012345678901"
  auth.mac.key    = "01234567890123456789012345678901"
}

# Web Server Settings
app.server {
  bind-address = "localhost"
  port = 8080
}
```

## 3. Create the Database Schema

Before we start the app, we need to create the tables. Run the following SQL in your PostgreSQL database:

```sql
CREATE TABLE tms_user (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE,
    full_name VARCHAR(100) NOT NULL,
    email VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE
);

CREATE TABLE project (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    owner_id INTEGER REFERENCES tms_user(id),
    status VARCHAR(20) DEFAULT 'PLANNING'
);

CREATE TABLE task (
    id SERIAL PRIMARY KEY,
    project_id INTEGER REFERENCES project(id),
    assignee_id INTEGER REFERENCES tms_user(id),
    summary VARCHAR(200) NOT NULL,
    details TEXT,
    due_date DATE,
    priority VARCHAR(10) DEFAULT 'MEDIUM', -- LOW, MEDIUM, HIGH
    status VARCHAR(20) DEFAULT 'OPEN'      -- OPEN, IN_PROGRESS, DONE
);
```

## 4. The Main Class

Create `src/main/scala/TMSApp.scala`.

```scala
package com.example.tms

import org.wabase.WabaseServer

object TMSApp extends App {
  // Initialize and start the Wabase server
  new WabaseServer {
    override def port = 8080
  }.start()
}
```

## 5. Verify Installation

Run the application:

```bash
sbt run
```

You should see logs indicating the server started on port 8080.
Go to `http://localhost:8080/` in your browser. You might see a 404 because we haven't defined any routes yet, but the server is running!

**Next Step:** [Defining Views and Basic CRUD](02-basic-crud.md)
