# Feature Guide Catalog

This catalog lists framework capabilities that benefit from dedicated, task-first guides (so developers can jump directly to a feature without scanning all reference docs).

## Dedicated Feature Guides

| # | Feature | Why Dedicated Guide Matters | Primary Runtime/Config Surface | Guide Status |
|---|---|---|---|---|
| 1 | Authentication and Sessions | Common first integration; cookie/session/jwt behavior is easy to misconfigure | `Authentication.scala`, `WabaseAuthentication.scala`, `session.*`, `jwt-decoder.*` | implemented |
| 2 | CSRF Protection | Browser security flow needs exact header/cookie contract | `CSRFDefence.scala`, csrf routes/tests | implemented |
| 3 | Views, Routes, and CRUD API | Core developer workflow and routing conventions | view YAML + route YAML + `doAction` | implemented |
| 4 | Action Language Workflows | High power surface (if/foreach/invoke/http/template/email/etc.) | `AppMetadata.scala`, `AppQuerease.scala` | implemented |
| 5 | Deferred Requests | Async behavior, polling contract, storage/timeout tuning | `DeferredControl.scala`, `WabaseDeferredControl.scala`, `app.deferred-requests.*` | implemented |
| 6 | Background Jobs and Scheduler | Operationally sensitive lock/run model | `WabaseScheduler.scala`, `QuartzScheduler.scala`, `app.job.*` | implemented |
| 7 | Auditing | Compliance-critical and performance-sensitive | `audit/Audit.scala`, `audit/BufferedAudit.scala`, `app.audit-*` | implemented |
| 8 | Internationalization (I18n) | User-visible behavior with locale/state interactions | `I18n.scala`, bundle resources, i18n routes | implemented |
| 9 | Files and Attachments | End-to-end file persistence and retrieval patterns | `AppFileStreamer.scala`, `file-streamer.*`, form/static routes | implemented |
| 10 | Templates and Document Generation | HTML/PDF output and template data contracts | `WabaseTemplate.scala`, template action | implemented |
| 11 | Email Sending | High-value app feature with multiple moving parts | `WabaseEmail.scala`, `Action.Email`, `app.email.*`, `simplejavamail.*` | implemented |
| 12 | Outbound HTTP Client Calls | External API integration and proxy behavior | `client/*.scala`, `Action.Http`, `http-client.*` | implemented |
| 13 | Server Notifications (SSE/WS) | Realtime integration with subscription semantics | `EventNotifications.scala`, `EventBus.scala`, `app.server-notifications.*` | implemented |
| 14 | API Metadata and Swagger | API contract publication and documentation pipeline | metadata/swagger handlers and generators | implemented |
| 15 | Request Decoding and Content Types | Payload parsing is common integration pain point | `RequestDecoder.scala`, parsers config | implemented |
| 16 | Result Rendering and Export | Output contracts and export formats | `ResultEncoder.scala`, renderers, spreadsheet exporters | implemented |
| 17 | Script Validations | Dynamic policy/validation hooks | `ScriptValidation.scala`, `app.script-validations.*` | implemented |
| 18 | Static Resources and Cache Controls | Frontend/static delivery and conditional requests | `getFromResource`, `CacheConditionHandlers.scala` | implemented |

## Scope Notes

1. These guides are task-first (how to implement feature X quickly and correctly).
2. Existing `docs/reference/*` remains canonical for low-level API/config details.
3. Each feature guide includes links back to relevant reference chapters and source files.
