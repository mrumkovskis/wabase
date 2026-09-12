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
Parameters are intended for i18n - error message is a static template with `%1$s`, `%2$s`, … placeholders,
which are replaced with parameter values on translation.
Validation messages are returned as objects with message and parameters instead of plain strings -
`org.mojoz.querease.ValidationResult.messages` is `List[ValidationMessage]` (previously `List[String]`),
where `ValidationMessage` has fields `msg` and `params`. Accordingly, json body of `400 Bad Request`
validation error response changed from `[{"location": [...], "messages": ["..."]}]` to
`[{"location": [...], "messages": [{"msg": "...", "params": [...]}]}]`, clients must be updated.
All messages of one validations step or view have the same parameter count, missing parameters are padded
with nulls. Parameters at the same position must be of compatible types across validations of a step or view.

Script (javascript) validation messages support parameters. Validation message may evaluate to string - message template
without parameters, array - message template followed by parameters, i.e. `['Should be %1$s, found %2$s', 43, my_int_field]`,
or object with `msg` and optional `params`, i.e. `{msg: 'Should be %1$s', params: [43]}`. Message which is not valid
javascript is used as is. Validation expression may evaluate to `true` - validation passes, `false` - fails with validation
message, array or object - fails with this message, validation message is not used, string - fails with message
`Error (validation "%1$s"): %2$s`, where first parameter is validation message as `{msg, params}` object and second one -
expression result (previously message text was concatenated).
Wrong validation definition is developer error and throws `RuntimeException` (http status `500 Internal Server Error`,
logged as error) instead of reporting validation error - expression evaluating to other value (i.e. `null`, `undefined`,
number), expression evaluation failure (previously `BusinessException`), message evaluating to other value or malformed
array or object. `BusinessException` thrown by custom function is propagated as is.
Custom functions `current_date()` and `now()` return strings in the format of date and timestamp variables, so they can be
compared with them (previously `java.sql.Date` and `java.sql.Timestamp` objects, which could not be compared in javascript).
Script validation documented in `docs/script-validation.md`.
Unused i18n resource `Validation error " %1$s ": Wrong validation result type: %2$s` removed.
