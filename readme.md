wabase [![Latest version](https://img.shields.io/maven-central/v/org.wabase/wabase_3)](https://central.sonatype.com/artifact/org.wabase/wabase_3)
![Build Status](https://github.com/mrumkovskis/wabase/actions/workflows/ci.yaml/badge.svg)
====

wabase is web application based on [Apache Pekko™](https://pekko.apache.org/) web server providing framework to develop json rest services based on sql databases.

## Main features

* [Routes](docs/routes.md) for [querease](https://github.com/guntiso/querease), [tresql](https://github.com/mrumkovskis/tresql) backed sql database calls.
* [View actions](docs/view-actions.md) and [action functions](docs/action-functions.md) for pluggable functional style business logic.
* [Swagger](docs/swagger.md) generated from routes and views.
* [File upload download](docs/routes.md#file) services linked with sql database.
* [Deferred](docs/routes.md#deferred) http request support.
* Data export in MS Excel, CSV, odt formats.
* Framework for request [audit](docs/routes.md#audit).
* Stateless [session](docs/routes.md#authentication) management.
* Dynamic data [validation in javascript](docs/script-validation.md).
* Framework for [I18n](docs/routes.md#i18n) support.

## getting started

```
sbt new https://github.com/guntiso/wabase-template.g8.git
```

Or add to your `build.sbt` file:

```
libraryDependencies += "org.wabase" %% "wabase" % "<version>"
```
