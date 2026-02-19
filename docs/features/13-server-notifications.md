# Server Notifications (SSE/WS)

## Use This When

You need real-time push updates over SSE or WebSocket.

## Core Surface

- `src/main/scala/EventNotifications.scala`
- `src/main/scala/EventBus.scala`
- `src/it/resources/views/server-events.yaml`

## Simple Example

```yaml
name: server_events
api: get
key: topic
fields:
- topic
get:
- wabase.app.EventsFunctions.subscribeToEvent :topic
```

## Complex Example

```yaml
name: server_events
api: get list save
key: topic
fields:
- topic
- value
get:
- wabase.app.EventsFunctions.subscribeToEvent :topic
list:
- wabase.app.EventsFunctions.subscribeToWsMessages :topic
insert:
- wabase.app.EventsFunctions.publishEvent(:topic, :value)
```

```hocon
app.server-notifications {
  enabled = true
  event-function = org.wabase.ServerNotifications.createServerEvent
  event-subscriber-watcher-actor-name = WabaseEventSubscriberWatcherActor
}
```

## Key Notes

1. SSE and WS can share event publication backend.
2. Use topic/user partitioning to limit noise.
3. Keep event payloads compact and versioned.

## Related Docs

- `../reference/09-async-jobs-deferred-events.md`
