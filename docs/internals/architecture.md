# Wabase Architecture

## Overview

Wabase follows a hierarchical object-oriented architecture:
`WabaseServer` -> `WabaseService` -> `WabaseApp` -> `AppQuerease` -> `Marshalling`

## Components

### 1. WabaseServer (`src/main/scala/WabaseServer.scala`)
Initializes the **Apache Pekko™** HTTP server and actors. It bootstraps the application and handles the main entry point.

### 2. WabaseService (`src/main/scala/WabaseService.scala`)
The HTTP handling layer.
*   **Routing**: Matches URL path to `RouteDef` (loaded from `routes.yaml`) using regular expressions.
*   **Handlers**: Defines handlers which can be chained. Handlers support dependency injection for parameters like `WabaseRequestContext`, `HttpRequest`, `WabaseUser`, etc.
*   **Marshalling**: Handles request/response serialization.
*   **Deferred Control**: Manages asynchronous long-running requests.
*   **Action Dispatch**: The standard `doAction` handler extracts view/action/key from the request and delegates to `WabaseApp`.

### 3. WabaseApp (`src/main/scala/WabaseApp.scala`)
The core application manager.
*   **Resources**: Manages `DbAccess`, `WabaseFileStreamers`, and `WabaseHttpClients`.
*   **Context**: Creates `AppActionContext` containing user, state, and parameters.
*   **Execution**: Runs Querease view actions.

### 4. AppQuerease (`src/main/scala/AppQuerease.scala`)
The business logic engine, extending `AppMetadata`.
*   **Metadata**: Loads and interprets Views and Actions.
*   **Execution**: `doActionOp` interprets the Action Language steps (Tresql, If/Else, Foreach, etc.).
*   **Integration**: Manages `WabaseTemplate` engine and `WabaseEmail` sender.
*   **Database**: Executes Tresql queries.

### 5. Tresql
The query generation and execution library. It maps the hierarchical View definition to SQL queries.

## Request Lifecycle

1.  **HTTP Request** -> `WabaseServer` -> `WabaseService.handle`
2.  **Route Match** -> `routes.yaml` -> Handler Chain identified.
3.  **Handler Execution**:
    *   Parameters injected (User, State, etc.).
    *   `doAction` handler invoked (typically).
4.  **WabaseApp.doWabaseAction** called.
5.  **AppQuerease.doAction** called.
6.  **Action Steps Execution**:
    *   Evaluation, Validation, Flow Control.
    *   Query execution via Tresql (JDBC).
    *   Scala/Java method invocation.
7.  **Result**: `QuereaseResult` (TresqlResult, Map, File, etc.)
8.  **Serialization**: `ResultSerializer` converts result to JSON stream or other formats.
9.  **HTTP Response** -> Client.
