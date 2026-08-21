wabase [![Latest version](https://img.shields.io/maven-central/v/org.wabase/wabase_3)](https://central.sonatype.com/artifact/org.wabase/wabase_3)
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

## getting started

```
sbt new https://github.com/guntiso/wabase-template.g8.git
```

Or add to your `build.sbt` file:

```
libraryDependencies += "org.wabase" %% "wabase" % "<version>"
```
