# Getting Started with Wabase

This guide will walk you through setting up a simple Wabase project.

## Prerequisites

*   **Java**: JDK 11 or higher.
*   **sbt**: Scala Build Tool.
*   **Database**: A supported SQL database (e.g., PostgreSQL, HSQLDB).

## Step 1: Create a new sbt project

Create a `build.sbt` file with the following dependencies:

```scala
name := "my-wabase-app"

version := "0.1"

scalaVersion := "2.13.12"

libraryDependencies += "org.wabase" %% "wabase" % "6.0.2"
// Add your database driver, e.g., for PostgreSQL:
libraryDependencies += "org.postgresql" % "postgresql" % "42.6.0"
```

## Step 2: Configure the Application

Create `src/main/resources/application.conf`. You need to configure at least the database connection.

```hocon
jdbc.cp {
  main {
    jdbcUrl = "jdbc:postgresql://localhost:5432/mydb"
    username = "myuser"
    password = "mypassword"
  }
}

app {
  host = "http://localhost:8080"

  # Security configuration (for development)
  auth.crypto.key = "change-me-to-a-random-string-32-chars-long"
  auth.mac.key    = "change-me-to-a-random-string-32-chars-long"
}
```

## Step 3: Define Metadata (Views)

Create `src/main/resources/views.yaml` to define your data model.

```yaml
table: person
columns:
- id
- name
- surname
- birthdate date

name: person
table: person
api: list, get, save, delete
fields:
- id
- name
- surname
- birthdate
```

## Step 4: Define Routes

Create `src/main/resources/routes.yaml` to map URL paths to actions.

```yaml
# Simple mapping for CRUD operations on 'person' view
on: /data/person
do:
  - org.wabase.WabaseServer.crudAction
```

## Step 5: Create the Main Application Class

Create a Scala object that extends `WabaseServer` to start the application.

```scala
package com.example

import org.wabase.WabaseServer

object MyApp extends App {
  // Start the server
  new WabaseServer {
    override def port = 8080
  }
}
```

## Step 6: Run the Application

Run the application using sbt:

```bash
sbt run
```

You can now access your API at `http://localhost:8080/data/person`.

*   **List**: `GET /data/person`
*   **Get**: `GET /data/person/{id}`
*   **Save**: `POST /data/person` (JSON body)
*   **Delete**: `DELETE /data/person/{id}`

## What's Next?

*   Learn more about **[Core Concepts](03-core-concepts.md)** like Views and Actions.
*   Explore **[Database Access](04-database-access.md)** with Tresql.
*   Dive into **[Advanced Features](05-advanced-features.md)**.
