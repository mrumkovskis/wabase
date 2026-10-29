# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
getByNameAction route uses application get method instead of list, so manager methods before after are on ViewContext instead of ListContext.
BorerNestedArraysTransformer decodes with `borer.transform.cbor.decoding` and `borer.transform.json.decoding` (same defaults as request decoding).

`acquireIsRunningLock` is the job status lock method. `acquireIsRunnningLock` remains and is deprecated.

File cleanup minimum age is `app.file-cleanup.min-age` (default 24 h).

`fold` variables use the same name parser as other op variables.

Op keywords match at a word boundary, so an identifier that starts with a keyword is left as an identifier.

Script validation binds a value whose name is not a JavaScript identifier, such as `old key`, as `this['old key']`.

`I18nService.applicationLocale` parses a language tag with a script, region, or variant (`en-US`, `zh-Hant-TW`). A tag that does not match this shape fails.

Audit of extra fields from a view stored in a non-default database uses that database name as the tresql prefix.

Tresql primary-key filters name the column. Audit extra-field load compares the single primary-key column, or `id`. Deferred request result update filters on `request_hash`.

`getObjectOrNewInstance` rethrows an exception thrown by the class constructor instead of trying other class name variants.
