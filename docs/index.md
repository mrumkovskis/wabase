# Wabase Documentation

Welcome to the definitive documentation for **Wabase**, the Scala-based web application framework designed for data-centric RESTful services.

## Overview

Wabase is a "database-first" framework. It assumes your data model (schema) is the source of truth and allows you to build a powerful REST API on top of it using declarative metadata (YAML) and a specialized query language ([Tresql](https://github.com/mrumkovskis/tresql)). It is built on the robust [Apache Pekko™](https://pekko.apache.org/) HTTP stack.

### Key Philosophy
*   **Metadata over Code**: Define *what* you want (views, actions), not *how* to do it.
*   **Hierarchical Data**: Handle complex nested JSON structures that map naturally to relational databases.
*   **Scalability**: Fully asynchronous and non-blocking.

## Documentation Structure

This documentation is divided into three main sections:

### 1. [The Comprehensive Guide](guide/01-setup.md)
A step-by-step tutorial that takes you from an empty project to a full-featured **Task Management System**.
*   [Setup & Installation](guide/01-setup.md)
*   [Basic CRUD](guide/02-basic-crud.md)
*   [Relationships & Validation](guide/03-relationships-and-validation.md)
*   [Files & Attachments](guide/04-files-and-attachments.md)
*   [Custom Actions](guide/05-custom-actions.md)
*   [Background Jobs](guide/06-background-jobs.md)
*   [Security & Roles](guide/07-security.md)

### 2. [Reference](reference/01-views.md)
Detailed technical specifications for every part of the framework.
*   [View Definition](reference/01-views.md)
*   [Action Language Spec](reference/02-action-language.md)
*   [Tresql Reference](reference/03-tresql.md)
*   [Configuration](reference/04-configuration.md)

### 3. [Internals](internals/architecture.md)
For advanced users who want to understand the engine under the hood.
*   [Architecture Overview](internals/architecture.md)
