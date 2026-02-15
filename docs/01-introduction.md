# Introduction to Wabase

Wabase is a powerful web application framework based on [Apache Pekko™](https://pekko.apache.org/) HTTP, designed to simplify the development of RESTful JSON services backed by SQL databases. It leverages [Tresql](https://github.com/mrumkovskis/tresql) for expressive database queries and [Querease](https://github.com/guntiso/querease) for metadata-driven application logic.

## Key Features

*   **Metadata-Driven**: Define your data models (views) and business logic in simple YAML files. Wabase automatically handles the rest.
*   **Database First**: Built on top of SQL databases with powerful querying capabilities via Tresql.
*   **RESTful APIs**: Automatically generates REST endpoints for your views (List, Get, Save, Delete).
*   **Customizable Logic**: Extend default behaviors with custom actions, validations, and complex workflows directly in metadata or Scala code.
*   **Asynchronous & Non-Blocking**: Built on Pekko HTTP for high performance and scalability.
*   **Built-in Services**:
    *   **File Handling**: Seamless upload/download linked to database records.
    *   **Deferred Requests**: Handle long-running tasks asynchronously.
    *   **Templating & Reporting**: Generate documents (PDF, Excel, etc.) using templates.
    *   **Email**: integrated email sending capabilities.
    *   **I18n**: Internationalization support.
    *   **Audit**: Automatic auditing of data changes.
    *   **Security**: Role-based access control and stateless session management.

## Architecture Overview

Wabase follows a layered architecture:

1.  **HTTP Layer (WabaseService)**: Handles incoming HTTP requests, routing, authentication, and response serialization.
2.  **Application Logic (WabaseApp)**: The core application logic that orchestrates actions based on metadata.
3.  **Query Engine (AppQuerease)**: Integrates Querease to interpret view definitions and execute database operations.
4.  **Database Access (Tresql)**: A query language for SQL databases that maps complex hierarchical data structures to JSON.

## Why Wabase?

Wabase is ideal for data-centric applications where you need to quickly expose database tables as REST APIs with complex validation and business rules. Its metadata-driven approach reduces boilerplate code significantly while allowing full flexibility to drop down to Scala code when needed.
