# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
getByNameAction route uses application get method instead of list, so manager methods before after are on ViewContext instead of ListContext.

Email action op has new `html` option - `email [batch] [html] <recipients> <subject> <body> (<attachment> [...])`.
Body is sent as html instead of plain text (no plain text alternative part is added).
Since `Action.Email` has new field, previously generated querease action cache (`querease-action-cache.cbor`)
must be regenerated.
