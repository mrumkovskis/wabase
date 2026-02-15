wabase [![Latest version](https://index.scala-lang.org/mrumkovskis/wabase/latest.svg)](https://index.scala-lang.org/mrumkovskis/wabase)
![Build Status](https://github.com/mrumkovskis/wabase/actions/workflows/ci.yaml/badge.svg)
====

wabase is web application based on [Apache Pekko™](https://pekko.apache.org/) web server providing framework to develop json rest services based on sql databases.

## Main features

* [Apache Pekko HTTP](https://pekko.apache.org/docs/pekko-http/current/) routes for [querease](https://github.com/guntiso/querease), [tresql](https://github.com/mrumkovskis/tresql) backed sql database calls.
* Framework for pluggable functional style business logic.
* File upload download services linked with sql database.
* Deferred http request support.
* Data export in MS Excel, CSV, odt formats.
* Framework for request audit.
* Stateless session management.
* Dynamic data validation in javascript.
* Framework for I18n support.

## Documentation

*   [Introduction](docs/01-introduction.md)
*   [Getting Started](docs/02-getting-started.md)
*   [Core Concepts](docs/03-core-concepts.md)
*   [Database Access](docs/04-database-access.md)
*   [Advanced Features](docs/05-advanced-features.md)
*   [Configuration](docs/06-configuration.md)

## use in your sbt project
add to your build.sbt file - libraryDependencies += "org.wabase" %% "wabase" % "6.0.2"
