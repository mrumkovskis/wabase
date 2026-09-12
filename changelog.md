# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
`unique` and `unique_opt` action ops throw specific exceptions instead of generic ones.
Empty row set for `unique` throws `org.mojoz.querease.NotFoundException` (previously
`NoSuchElementException`), which is mapped to http status 404 by default exception handlers.
More than one row for `unique` and `unique_opt` throws new `org.wabase.NotUniqueException`
(previously `org.tresql.TooManyRowsException` or `RuntimeException`), which is not mapped to
any status code, i.e. results in http status 500. Applications can map it to some other
status code, for example 409, by adding own exception handler.
Also `unique` and `unique_opt` on stream (`foreach`) result throw on more than one element
instead of silently returning the first one.

getByNameAction route uses application get method instead of list, so manager methods before after are on ViewContext instead of ListContext.

Email action op has new `html` option - `email [batch] [html] <recipients> <subject> <body> (<attachment> [...])`.
Body is sent as html instead of plain text (no plain text alternative part is added).
Since `Action.Email` has new field, previously generated querease action cache (`querease-action-cache.cbor`)
must be regenerated.

Querease upgraded to 11.0.0. Validations accept error message parameter expressions following the
error message expression - `[<cursor definitions>, ] <require condition>, <error message> [, <message parameter> …]`.
Validation messages are returned as objects with message and parameters instead of plain strings -
`org.mojoz.querease.ValidationResult.messages` is `List[ValidationMessage]` (previously `List[String]`),
where `ValidationMessage` has fields `msg` and `params`. Accordingly, json body of `400 Bad Request`
validation error response changed from `[{"location": [...], "messages": ["..."]}]` to
`[{"location": [...], "messages": [{"msg": "...", "params": [...]}]}]`, clients must be updated.
All messages of one validations step or view have the same parameter count, missing parameters are padded
with nulls. Parameters at the same position must be of compatible types across validations of a step or view.
