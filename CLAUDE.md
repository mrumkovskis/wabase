# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Wabase is a **Scala web application framework** built on Apache Pekko HTTP that provides REST API infrastructure for JSON services backed by SQL databases. It is a **library** published to Maven Central, not a standalone application. Cross-compiled for Scala 2.12, 2.13, and 3.3.

Key dependencies: Apache Pekko HTTP (routing), TreSQL (SQL DSL), Querease/Mojoz (metadata-driven queries), HikariCP (connection pooling), HSQLDB (testing).

## Build Commands

Requires **Java 11** (enforced at build time).

```bash
# Full CI suite (unit + integration tests, all Scala versions)
sbt -Dhsqldb.method_class_names="test.HsqldbCustomFunctions.*" clean update +compile +test:compile +test +it/test:compile +it/test +it_legacy/test:compile +it_legacy/test +versionPolicyCheck

# Compile only
sbt compile

# Run unit tests (current Scala version)
sbt test

# Run a single test class
sbt "testOnly org.wabase.CrudServiceSpecs"

# Run a specific test by name (use single quotes; FlatSpec test name includes the subject, e.g. "actions should rollback transaction")
sbt 'testOnly org.wabase.CrudServiceSpecs -- -t "<subject (value of behavior of)> should <test name here>"'

# Run integration tests
sbt "it/test"
sbt "it_legacy/test"

# Run a single integration test
sbt "it/testOnly org.wabase.integration.SomeSpec"

# Check binary compatibility
sbt versionPolicyCheck
```

Test reports are written to `report/` (unit) and `wabase-it-report/` (integration).

## Architecture

### Trait-Based Composition

The framework is built entirely from **stackable Scala traits**. Applications compose their own class by mixing in the traits they need. The central composition point is `WabaseApp`, which aggregates:

- **`AppBase`** — Core CRUD actions (Get, List, Insert, Update, Delete, Save, Create, Count)
- **`AppQuerease`** (`AppQuerease.scala`, 101KB) — Translates action calls into TreSQL/Querease queries; the largest and most complex file
- **`AppMetadata`** (`AppMetadata.scala`, 87KB) — Route definitions, view metadata, field ordering, authorization rules parsed from YAML/YML definitions
- **`DbAccess`** — HikariCP connection pool management; thread-local TreSQL resources; supports multiple named pools
- **`WabaseAuthentication`** — Stateless session/JWT-based authentication
- **`Authorization`** — Role-based access control evaluated against metadata
- **`Marshalling`** — Pekko HTTP JSON marshalling/unmarshalling using spray-json
- **`Audit`** — Request/response audit trail recording to a dedicated DB pool
- **`I18n`** — Internationalization support

### Request Flow

```
HTTP Request
  → WabaseServer (Pekko HTTP binding, SSL)
  → WabaseService (route dispatcher)
  → WabaseAuthentication (session extraction)
  → Authorization (role check vs AppMetadata)
  → AppBase CRUD method
  → AppQuerease (builds TreSQL query)
  → DbAccess (executes via thread-local TreSQL resources)
  → Marshalling / ResultSerializer (streams JSON response)
```

### Key Files

| File | Role |
|------|------|
| `WabaseServer.scala` | HTTP server bootstrap, port binding, SSL/TLS, graceful shutdown |
| `WabaseService.scala` | Main Pekko HTTP route tree, error handling, deferred request dispatch |
| `WabaseApp.scala` | Trait composition root combining all framework traits |
| `AppBase.scala` | Core CRUD operation dispatch; legacy handler bridge |
| `AppQuerease.scala` | Querease/TreSQL query construction and execution (**101KB**) |
| `AppMetadata.scala` | YAML-driven metadata: routes, views, fields, auth rules (**87KB**) |
| `DbAccess.scala` | Database access: HikariCP pools, TreSQL resources, timeout management |
| `RequestDecoder.scala` | HTTP body/parameter unmarshalling |
| `Marshalling.scala` | JSON serialization, custom type handlers |
| `ResultSerializer.scala` | Streaming result serialization for large datasets |
| `AppServiceBase.scala` | Service base with exception mapping and error responses (**40KB**) |
| `AppFileStreamer.scala` | File upload/download linked to DB records |
| `AppFileCleanup.scala` | Automatic orphaned file cleanup |
| `DeferredControl.scala` | Long-running request deferral and status polling |
| `Authentication.scala` | JWT decoding, LDAP, and crypto utilities |
| `YamlRouteDefLoader.scala` | YAML route definition parsing |
| `ds/HikariDs.scala` | HikariCP configuration from HOCON |
| `ds/ConnectionPools.scala` | Multi-pool lifecycle management |
| `swagger/WabaseSwaggerGenerator.scala` | OpenAPI 3 spec generation from metadata |

### Test Projects

- `src/test/` — Unit tests (ScalaTest FlatSpec); uses HSQLDB in-memory
- `src/it/` — Integration tests; current/main IT suite
- `src/it_legacy/` — Integration tests for backward compatibility scenarios

The `hsqldb.method_class_names` system property must be set when running tests to register custom HSQLDB functions defined in `test.HsqldbCustomFunctions`.

### Configuration

Runtime configuration uses HOCON (`reference.conf` ships with the library). Key sections:

```hocon
app {
  home = "/path/to/app"
  files.path = ${app.home}/files
  port = 8080
  upload.size-limit = 8m
  deferred-requests { enabled = true, worker-count = 2, default-timeout = 180s }
}
jdbc.cp {
  main { ... }   # HikariCP config blocks, one per named pool
}
```

The `audit-pool-name` config key points to a separate connection pool used exclusively for audit writes.
