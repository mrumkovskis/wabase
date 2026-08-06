# Email Sending

## Use This When

You need transactional or batch emails with optional attachments.

## Core Surface

- `src/main/scala/WabaseEmail.scala`
- `src/main/scala/AppMetadata.scala` (`email` op parser)
- `src/main/scala/AppQuerease.scala` (`doEmail`)
- `src/test/resources/querease-action-specs-metadata.yaml` (`email_test1`)

## Simple Example

```yaml
insert:
- email {'user@example.com' to} 'Welcome' 'Your account is ready.'
- status ok
```

```hocon
app.email {
  enabled = true
  sender = org.wabase.DefaultWabaseEmailSender
}
```

## Complex Example

```yaml
insert:
- recipients = to file ({ 'a@a.a' 'to', 'Ana' name } + { 'b@b.b' 'to', 'Bob' name }) content_type='text/csv; charset=UTF-8'
- invoice = to file (template '<h1>Invoice {{id}}</h1>' data={:order_id id}) filename='invoice.html' content_type='text/html; charset=UTF-8'
- email batch
    extract entity using default_csv_decoder file {:recipients.id, :recipients.sha_256}
    (template 'Order {{order_id}} confirmation' { :order_id order_id })
    (template 'Hi {{name}}, your order is processed.' data={:name name})
    (file {:invoice.id, :invoice.sha_256})
```

```hocon
simplejavamail {
  smtp.host = "smtp.example.com"
  smtp.port = 587
  smtp.username = "mailer-user"
  smtp.password = "mailer-password"
  transportstrategy = "SMTP_TLS"
}
```

## Key Notes

1. Recipient field `to` is required; `cc`, `bcc`, `from`, `replyTo` are optional.
2. `email batch` iterates over recipient rows.
3. Attachment ops are rendered and attached as streamed content.

## Related Docs

- `../reference/02-action-language.md`
- `../reference/09-async-jobs-deferred-events.md`
- `../reference/04-configuration.md`
