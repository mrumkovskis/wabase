# Wabase Architecture

## Components

### 1. WabaseService (`src/main/scala/WabaseService.scala`)
This is the HTTP layer based on Pekko HTTP.
*   **Routing**: Matches URL path to `RouteDef` (loaded from `routes.yaml`).
*   **Request Handling**: Parses headers, cookies, and body.
*   **Authentication**: Calls `WabaseAuthentication`.
*   **Dispatch**: Calls `WabaseApp.doAction`.

### 2. WabaseApp (`src/main/scala/WabaseApp.scala`)
The central orchestrator.
*   **Action Context**: Creates `AppActionContext` containing user, state, and parameters.
*   **Action Dispatch**: Routes action (e.g., "save") to specific handlers.
*   **Result Handling**: Serializes results (JSON, Stream) using `AppQuerease`.

### 3. AppQuerease (`src/main/scala/AppQuerease.scala`)
The engine logic.
*   **Metadata**: Loads `AppMetadata` (views, actions).
*   **Execution**: `doActionOp` interprets the action steps.
*   **Tresql**: Executes Tresql queries against the database.
*   **Validation**: Runs validation logic.

### 4. Tresql
The query generation and execution library. It maps the hierarchical View definition to SQL queries.

## Request Lifecycle

1.  **HTTP Request** -> `WabaseService.handle`
2.  **Route Match** -> `routes.yaml` -> View/Action identified.
3.  **Auth** -> User identified.
4.  **WabaseApp.doAction** called.
5.  **AppQuerease.doAction** called.
6.  **Steps Execution**:
    *   Validation? -> `AppQuerease.doValidationStep`
    *   Query? -> `AppQuerease.doTresql` -> JDBC
    *   Code? -> `AppQuerease.doInvocation` -> Reflection
7.  **Result**: `QuereaseResult` (TresqlResult, Map, File, etc.)
8.  **Serialization**: `ResultSerializer` -> JSON Stream.
9.  **HTTP Response** -> Client.
