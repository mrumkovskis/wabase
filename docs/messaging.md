# Messaging

View actions can read messages from and send messages to a messaging service (Kafka is supported out of the
box). Messages can be processed in action steps, sent to other topics, streamed to http clients as server sent
events and committed, so that processing continues where it stopped.

- [Overview](#overview)
- [Configuration](#configuration)
  - [Providers](#providers)
  - [Kafka provider](#kafka-provider)
  - [Codecs](#codecs)
  - [Evaluator connection pool](#evaluator-connection-pool)
- [Reading messages](#reading-messages)
  - [Subscription](#subscription)
  - [Stream termination](#stream-termination)
  - [Message bind variables](#message-bind-variables)
- [Processing messages - foreach](#processing-messages---foreach)
- [Committing](#committing)
- [Database resources](#database-resources)
- [Sending messages](#sending-messages)
  - [Parameter evaluation](#parameter-evaluation)
  - [Send filter](#send-filter)
- [Multi send - fan-out](#multi-send---fan-out)
- [Restart on failure](#restart-on-failure)
- [Server sent events](#server-sent-events)
- [Admin calls](#admin-calls)
- [Headers](#headers)
- [Parameter evaluation](#parameter-evaluation)
- [Provider API](#provider-api)
- [Limitations](#limitations)
- [Future improvements](#future-improvements)

Operation syntax is described in [view actions](view-actions.md#msg-read).

## Overview

```yaml
name: order_events
api: get
key: group
fields:
- group
get:   # server sent events of new orders, committed - client continues where it stopped
- foreach msg read group=:group from='orders' committer=true:
  - unique {:id id, :status status}
```

```yaml
name: order_forwarder
api: post
post:  # forward orders to other topic until message with status 'end', restart on failure
- msg send to='orders_copy' restart={'1s' min_backoff, '30s' max_backoff}:
  - foreach msg read group='forwarder' from='orders' committer=true to={:status = 'end'}:
    - if {:status != 'draft'}:   # draft orders are not forwarded, but committed
      - unique {:id id, :status status}
```

- `msg read` creates a stream of incoming messages - it is lazy, messages are read when the result is consumed:
  by `msg send`, by the http response (server sent events), by a following step (collected into a list) or by
  `foreach` with `fold`.
- `foreach` on `msg read` processes each message and returns stream of processing results.
- `msg send` sends result of its operation (message stream, rows or single value) as messages.
- With `committer` messages are committed after they are processed (sent, emitted to http response) - delivery is
  *at least once*: after failure messages processed, but not committed, are delivered again.

## Configuration

### Providers

Messaging providers are configured in `msg-provider` section, each object is a provider, `main` is the default one.
Provider settings are inherited from `msg-provider` section:

```
msg-provider {
  factory-class = org.wabase.msg.kafka.KafkaProvider   # provider factory, inherited by providers
  codecs {                                             # named codecs, inherited by providers
    avro { factory-class = com.example.AvroCodec, schema-registry-url = "..." }
  }
  key-codec   = string                                 # default key codec
  value-codec = json                                   # default value codec
  main {                                               # default provider
    bootstrap-servers = "localhost:9092"
  }
  other {                                              # provider used as msg read [other] ...
    bootstrap-servers = "other:9092"
    value-codec       = avro
  }
}
```

- `factory-class` - class or object with `create` function returning `org.wabase.msg.MsgProvider`, function can
  declare parameters `ActorSystem`, `ExecutionContext`, `Config` (provider config), `String` (provider name).
- objects listed in `msg-provider.common-setting-objects` (`codecs`, `key-codec`, `value-codec`, `consumer`,
  `producer`, `committer`, `admin`) are settings inherited by providers, not providers.
- providers are created on application start (`WabaseApp.msgProviders`) and closed on actor system shutdown
  (coordinated shutdown phase `service-stop`) - running consumers are stopped.

### Kafka provider

`org.wabase.msg.kafka.KafkaProvider` is based on pekko-connectors-kafka, which is an optional dependency of wabase -
add it to the application:

```scala
libraryDependencies += "org.apache.pekko" %% "pekko-connectors-kafka" % "1.2.0"
```

```
msg-provider.main {
  factory-class     = org.wabase.msg.kafka.KafkaProvider
  bootstrap-servers = "localhost:9092"  # optional, overrides bootstrap.servers of kafka-clients
  group-id          = my-app            # optional, default group if msg read does not specify it
  stop-timeout      = 30s               # optional, overrides consumer.stop-timeout, see below
  consumer  { }                         # falls back to pekko.kafka.consumer
  producer  { }                         # falls back to pekko.kafka.producer
  committer { }                         # falls back to pekko.kafka.committer
  admin.kafka-clients { }               # kafka admin client properties
}
```

- auto commit of kafka consumer is disabled, messages are committed only with `committer`.
- producer and admin client are shared by provider and created on first use.
- consumer `stop-timeout` - how long a cancelled consumer waits for pending commits before it closes (pekko default
  30s, the consumer keeps its partitions meanwhile, so the next read of the same group waits). Msg operations do not
  need it - consumers of plain and at most once reads have nothing to wait for, committing reads are stopped only after
  all commits are done, so `stop-timeout` is 0 for them. It applies to direct provider API users only.
- typical consumer settings: `consumer.kafka-clients.auto.offset.reset = earliest` - where to start if group has no
  committed offset.

### Codecs

Message keys and values are converted to and from bytes by codecs. A codec is resolved separately for key and value:

1. `using` parameter of `msg read` / `msg send`,
2. `key-codec` / `value-codec` of provider config (`msg-provider.<provider>`, falling back to `msg-provider`),
3. built-in default - `string` for key, `json` for value.

Codec is specified by name or as object:
- name - codec configured in `codecs` or built-in codec: `json`, `cbor`, `string`, `bytes` (configured codec may
  override built-in one),
- object `{ factory-class = ... }` - scala object or class implementing `org.wabase.msg.MsgCodec` with no-arg or
  `(Config)` constructor, codec config is passed to constructor. Codecs with settings (schema registry etc.) are
  configured in `codecs` and referenced by name.

Codec receives topic and headers, so it can choose schema or format per topic or by header (e.g. `content-type`) -
format varying per message is handled by such codec, not by `using`. Content type headers are not respected by
default. If the same codec class is used for key and value and must distinguish them, use codec config
(e.g. `is-key = true`).

Decoding failure fails the stream with `MsgDecodingException` (with topic, partition, offset), encoding failure with
`MsgEncodingException`.

`using` parameter - tresql returning codec names, evaluated once per operation for `msg read`, for each message
for `msg send` (see [parameter evaluation](#parameter-evaluation)):

```
using='avro'                        # value codec avro, key codec from configuration
using={'string' key, 'avro' value}
using={:format value}               # value codec from bind variable, from configuration if null
```

- column `key` - key codec name, column `value` - value codec name, both optional, single unaliased value is value
  codec name,
- `null` or missing column - codec is resolved from configuration,
- unknown name fails the operation with `Message codec 'x' not found. Available codecs: [...]`.

### Evaluator connection pool

Tresql evaluated per message (`to`, `commit_metadata`, foreach body, send `filter`) runs on the operation's database
connection (see [database resources](#database-resources)). Only when there is none - view with `explicit db: true`
outside `db` / `transaction` - it uses connection from evaluator pool `app.wabase.evaluator.pool` (default
`_evaluator_`), which must then be configured in `jdbc.cp`, otherwise the stream fails with `Unable to find
connection pool`. A connection is borrowed for each evaluation - size the pool accordingly.

## Reading messages

```
msg read [<provider>]
  [group=<group name>]
  from=<subscription rows>
  [to={<boolean expression>}]
  [timeout={<duration> within, <duration> idle}]
  [committer=<committer settings or true>]
  [commit_metadata=<string expression>]
  [key=<key variable name>]
  [using=<codec names>]
  [restart=<restart settings or true>]
  [at_most_once=<true|false>]
  [decode_error=<'fail'|'skip'>]
```

See [view actions](view-actions.md#msg-read) for parameter syntax. `msg read` returns a messaging result - a stream
of incoming messages. Each message is a map `{topic, partition, offset, timestamp, key, headers, value}` when
collected or sent as server sent event.

### Subscription

`from` rows specify what is read, all rows must have the same columns (null columns are ignored):

| Columns | Reads | Group |
|---|---|---|
| `topic` | topics, partitions are assigned among group members | required |
| `pattern` | topics matching regular expression | required |
| `topic, partition` | partitions, from committed offset or as configured (`auto.offset.reset`) | if committed |
| `topic, partition, offset` | partitions, from offsets | if committed |
| `topic, partition, timestamp` | partitions, from first messages with timestamp >= specified | if committed |

```
from='orders'                                         # topic
from={'orders' topic} ++ {'returns' topic}            # topics
from={'orders-.*' pattern}                            # topics matching pattern
from={'orders' topic, 0 partition, 100 offset}        # partition from offset
from=msg_topic[active]{name topic}                    # from database table
```

`group` - consumer group, defaults to provider `group-id`. Group members share partitions of subscribed topics -
for example, http clients streaming the same group get different partitions.

### Stream termination

Without `to` or `timeout` the stream is infinite - it ends when its consumer cancels it (http client disconnects) or
the provider is closed. Such a stream can be streamed (server sent events, `msg send`), but not collected.

- `to={<boolean expression>}` - evaluated for each message, the message for which it is true is the last one
  (inclusive). Comparison must be enclosed in braces - top level `:value = 'end'` is tresql assignment.
- `timeout={<duration> within}` - total stream duration.
- `timeout={<duration> idle}` - max time waiting for the next message, also if no message arrives at all. Time
  spent processing messages (slow foreach body, slow http client) is not counted.

Durations are strings like `5s`, `100 millis` or numbers of milliseconds. The stream completes normally when `to`
matches or a timeout expires. Timers start with the stream - for group subscriptions (`topic`, `pattern`) they include
group join and partition assignment, which can take seconds, so `idle` must be longer than that (partition
assignment has no group join).

Messages already fetched by consumer after the stream is stopped are not emitted and not committed - the next read
delivers them.

### Message bind variables

Tresql evaluated per message (`to`, `commit_metadata`, foreach body) gets message bind variables:

- map value - its fields, other value - `value`; with foreach item variable (`foreach m in msg read ...`) - value
  under item variable,
- `__metadata` - map `{topic, partition, offset, timestamp, key, headers}`, headers are list of `{name, value}`,
- `__idx` - message index (foreach body only), filtered and skipped messages are not counted,
- message key under name specified by `key` parameter, if value has no such field.

Like foreach item scope, the scope is not transparent - operation scope variables are accessible with `..` prefix,
e.g. `to={:value = :'..'.last}`.

## Processing messages - foreach

```
- foreach msg read from={'orders' topic, 0 partition} to={:status = 'end'}:   # body for each message
  - :id || ' ' || :status
- foreach m in msg read from='orders' group='g':             # item variable
  - unique {:m.id id, :__metadata.offset offset}           # unique - single row as map
- total = 0
- foreach msg read from='payments' group='g' timeout={'5s' idle} fold (total, amount) {:total + :amount}:
  - :amount
```

- foreach on `msg read` (or on other foreach on `msg read`) returns messaging result of body results - one element
  per message. A body returning several rows produces one element (list), use `unique` to get a single row as map.
- body returning `null` or no result (e.g. `if` without `else` with false condition) filters the message out - it is
  not emitted, but it is committed.
- messages are processed one by one in offset order.
- `fold` runs the stream during the action (with commits), result is fold value.

## Committing

```
committer=true                                        # commit with provider settings
committer={100 max_batch, '5s' max_interval}
commit_metadata=('processed by ' || :'..'.node)       # committed together with offset
```

- `committer` columns `delivery` (`WaitForAck|SendAndForget`), `max_batch`, `max_interval`, `parallelism`,
  `commit_when` (`OffsetFirstObserved|NextOffsetObserved`) override provider `committer` settings.
- messages are committed after they are processed - sent by `msg send`, emitted to http response, collected, folded.
  Filtered messages are committed too, in offset order.
- delivery is at least once - after a failure messages processed but not committed are delivered again, so
  processing should be idempotent (upsert, deduplication by topic / partition / offset).
- `at_most_once=true` - messages are committed before they are emitted, a message may be lost after failure.
  Cannot be combined with `committer`.
- with server sent events messages are committed when they are emitted to the response stream, not when the
  client receives them - events in flight of a disconnected client may be lost.

### Decoding errors

A message whose key or value cannot be decoded (e.g. invalid json) fails the stream with message position in
the error message. The failure is permanent - the message fails again when it is read again, so it is not
restarted by `restart`, and a committing consumer cannot get past the message.

- `decode_error='fail'` - default, the stream fails.
- `decode_error='skip'` - the message is logged (warning with topic, partition, offset) and skipped: it is not
  emitted, `to` and foreach body are not evaluated for it, with `committer` it is committed in offset order like
  a filtered message (with `commit_metadata` - with metadata of the previous message of its partition). The
  message is lost - use it when undecodable messages can be ignored.
- `to` cannot match a skipped message - if the last message expected by `to` cannot be decoded, the read does not
  end, so specify `timeout` as well. Without `committer` skipped messages do not restart `timeout idle`.
- skipped messages are not counted by `__idx`.

The default can be set in provider configuration (`decode-error = skip`), the parameter overrides it.

## Database resources

Messaging operations use database resources of the operation - action's connection, or of enclosing `db` /
`transaction` block. Parameters evaluated once (`from`, `group`, `committer`, ...) and tresql evaluated per message
(`to`, `commit_metadata`, foreach body, `msg send` parameters) run on that connection and may access its database. Without a
connection (`explicit db: true` view outside `db` block) tresql runs on [evaluator
connection](#evaluator-connection-pool).

Messaging result consumed within the action (`msg send`, `msg multi send`, foreach with fold, assignment to variable)
uses the connection while the action runs, as other steps. Messaging result returned by the action (server sent
events, job) or by a `db` / `transaction` block is consumed after it has completed - the connection is kept until the
message stream terminates (completes, fails or is cancelled, e.g. client disconnects) and is released then: the
transaction is committed, or rolled back if the stream fails. Therefore:

- the transaction stays open while the stream runs - writes of the action done before `msg read` (and of foreach
  body) are not visible to others and their locks are held until the stream terminates, a stream failure rolls them
  back (messages may already be sent or committed by then); a continuous stream holds a pooled connection per
  subscriber. Manage it - write in own `db` / `transaction` block, which commits per message before the message
  offset is committed, use `explicit db: true` so that a stream holds no connection (per message tresql then runs on
  evaluator connection), or keep the stream short (`to`, `timeout`):

```
- foreach msg read group='g' from='orders' committer=true:
  - transaction:
    - =order[id = :id] {status = :status}
```

- per message tresql stages may use the connection concurrently (e.g. `to` of the next message while foreach body
  processes the previous one) - supported by common JDBC drivers, statements are serialized on the connection.
- messaging result is consumed once - `msg send` `restart` evaluates the operation again, with new resources of
  `db` blocks; `msg read` `restart` runs the stream again with the same resources, they are released after the
  last attempt.
- the connection of a returned messaging result which is never run (e.g. response not sent) is not released.
- in a returned stream (server sent events) foreach body writes must be in own `transaction` / `db` block - when
  the stream is cancelled (client disconnects), the connection is committed and released without waiting for the
  message being processed, so its work running on the action's connection may be partially committed (statements
  done before the release), the rest fails. The message is not committed and is processed again on the next read.
  Read only per message tresql (`to`, `commit_metadata`, queries in body) is not affected.

## Sending messages

```
msg send [<provider>]
  to=<topic | {topic, partition, timestamp}>
  [key={<field name> name | <key value> value, <true|false> should_remove}]
  [headers={<name> name, <value> value} ++ ...]
  [using=<codec names>]
  [restart=<restart settings or true>]
  [filter={<boolean expression>}]
  <operation>                                           # or block - ':' followed by steps
```

See [view actions](view-actions.md#msg-send) for parameter syntax. Operation result is sent:

- messaging result (`msg read`, foreach on `msg read`), stream, rows, collection - each element as a message,
- single value (map, scalar) - one message,
- `null` elements and no result - nothing is sent.

Incoming message (`msg read` without foreach) is forwarded - its value is sent, its key is kept unless `key` is
specified. Committing reads are committed after messages are sent, in offset order.

`key` - `name` takes the key from a field of map value (`should_remove` removes the field from the value), `value`
is the key itself. Key is encoded with key codec - `string` by default, use for example `using={'json' key}` for map
keys. Header values must be strings, bytes or null, other values are converted to string.

### Parameter evaluation

`restart` is evaluated once, before the operation. Other parameters are evaluated for each element - `filter` first,
then `to`, `key`, `headers`, `using` for elements which are sent, so that topic, partition, key, headers and codecs
may depend on the element:

```
- msg send to=:'..'.topic (:value)                             # operation variable - '..' prefix
- msg send to=('orders-' || :region) key={:id || '-' || :line value} headers={'type' name, :type value}
    using={:format value} msg read group='router' from='orders' committer=true
```

- bind variables - as of [send filter](#send-filter): for incoming message (`msg read` without foreach) [message
  bind variables](#message-bind-variables), for other elements map fields or `value`. Scope is not transparent -
  operation scope variables (e.g. view fields) are accessible with `..` prefix only (`to=:'..'.topic`).
- why `..` - like foreach item, element scope is created by data: its variable names come from the element (row,
  message from external producer), not from the view. With a transparent scope a field missing in the element would
  silently resolve to an operation variable of the same name, and message content could replace an operation
  variable - e.g. a producer adding `topic` field would decide the destination of `to=:topic`. Element and
  operation variables are therefore separate. `msg read` parameters are evaluated once in operation scope, so
  they access operation variables directly (`from={:topic topic}`), while `msg send` parameters, evaluated per
  element, use `..` for the same variable.
- constant parameters are evaluated for each element too - each parameter is tresql evaluation with operation's
  [database resources](#database-resources) (parameters of an element are evaluated on one connection).
- invalid parameter (e.g. missing topic, unknown codec, missing bind variable - operation variable without `..`)
  fails the stream when the first element is sent, not before the operation. Parameter error is permanent - it is
  not restarted with `restart`, the operation fails with it.

### Send filter

`filter` is evaluated for each element, element is sent only if it is true (`null` is false). Filtered incoming
message is committed, like message filtered out by foreach body:

```
- msg send to='orders_eu' filter={:region = 'eu'} msg read group='eu' from='orders' committer=true
- msg send to='active_users' filter={:active} users{id, name, active}
```

- bind variables - for incoming message (`msg read` without foreach) [message bind
  variables](#message-bind-variables) (value fields, `__metadata`, message key under `msg read` `key` name), so
  forwarded messages are filtered without transformation, keeping their key; for other elements (foreach result,
  rows) - map fields or `value`, like foreach item. Operation scope variables are accessible with `..` prefix.
  Incoming message metadata is not available after foreach - use `if` in foreach body instead.
- comparison must be enclosed in braces, like `msg read` `to`.
- evaluated with operation's [database resources](#database-resources) - concurrently with reading of query result
  rows on the same connection, JDBC driver must be thread safe.

Result is sent message count. The operation completes when all messages are sent and committed - with an infinite
stream never (e.g. a job forwarding messages).

Query result rows are sent as they are read from the cursor, they are not collected into memory. For large results:
- `tresql.max-result-size` does not apply to queries of the operation (also in nested `db` / `transaction`) - its
  result is streamed. Intermediate results collected by operation steps are not limited either.
- JDBC driver may still fetch all rows at once - for example PostgreSQL fetches rows in batches only with autocommit
  off and `tresql.fetch-size` (`tresql.<db>.fetch-size`) set.

## Multi send - fan-out

Sends each element of the source operation to several destinations - routing (destination filter or operation),
copying, transformation per destination, splitting:

```
msg multi send [restart=<restart settings or true>]:
  - <source operation step>
  - …
  - to:
    - msg send [<provider>] to=… [key=…] [headers=…] [using=…] [filter=…] [<operation>]
    - msg send … :                                      # or block
      - <expression>
    - …
```

```
- msg multi send restart=true:
  - msg read group='router' from='orders' committer=true
  - to:
    - msg send to='orders-eu' key={'id' name}:                        # adjusted per destination
      - if {:region = 'eu'} (unique {:id id, :amount amount, 'EUR' currency})
    - msg send to='orders-us' filter={:region != 'eu'} (unique {:id id, :amount amount})
    - msg send [other] to='audit'                                     # element as is, other provider
- db use:                                                             # source and destinations share connection
  - msg multi send:
    - foreach audit_log[time > :since]{id, action, user_id}:          # any source - db rows, http, foreach
      - unique {:id id, :action action, :user_id user_id}
    - to:
      - msg send to='audit' key={'id' name}
      - msg send to='user_events' filter={:user_id != null}
```

Block - source operation steps followed by `to` block with destinations. `to` has this meaning only as the last step
of `msg multi send` block, elsewhere it is a named block. Multi send has no single operation form.

Source - result of source steps, as `msg send` operation: messaging result (`msg read`, foreach on it), stream,
rows, collection, single value. Foreach is optional - only for processing common to all destinations (e.g. db write
done once). Query result size limit does not apply, as for `msg send`.

Destination - `msg send` with its parameters, evaluated for each element as of [msg send](#parameter-evaluation),
and optional operation; `restart` is not allowed, it is specified on `msg multi send`:
- operation is body of implicit foreach over source elements - bind variables as of [send filter](#send-filter)
  (message bind variables for incoming message, otherwise map fields or `value`), `__idx`, parent scope with `..`;
  without operation the element is sent as is,
- operation result `null` or no result - nothing is sent to the destination; rows - each row is a message,
  collection - each element is a message (to send a list as one message, wrap it in a map, e.g.
  `unique {:items items}`),
- operation result is collected per element before sending - large data belongs to the source, which is streamed
  (e.g. `foreach` over rows as source instead of destination operation returning many rows),
- incoming message key is retained unless `key` is specified, with or without operation,
- `filter` is evaluated first, then parameters (`to`, `key`, `headers`, `using`), then operation - with the same
  element bind variables (`__idx` included).

Delivery:
- for each element destinations are evaluated in order (filters and operations), then messages are sent to
  destinations in order; incoming message is committed after it is sent to all destinations (also if filtered
  out) - at least once across destinations: after restart a destination may receive a message again. Side effects
  of destination operations (db writes) are at least once too.
- `restart` - only with messaging source, restarts reading, processing, sending and committing, as `msg send`.
- non messaging source (db rows, http) - sending is not tied to the source, after failure messages may already be
  sent to some destinations; mark rows as sent in the same transaction, or use transactional outbox with messaging
  source.

Database resources of destination filters and operations - action's resources, like foreach body (action waits for
the send), see [database resources](#database-resources). With query source, filters and operations run concurrently
with reading of query result rows on the same connection - JDBC driver must be thread safe. Wrap multi send in
`db use` / `transaction` so that source and destination operations use the same database (`db` around the source
covers only the source). Some JDBC drivers do not support statements while a result set is open (e.g. MySQL
streaming result sets) or read the rest of the result into memory (SQL Server without `selectMethod=cursor`).

Result - rows `{provider, topic, count}` of sent message counts per destination and topic, in destination order,
topics of destination in order of the first sent message; destination without sent messages - one row with `null`
topic and 0 count (several destinations may have the same topic). The operation completes when all messages are sent and committed.

## Restart on failure

```
restart=true                                          # defaults
restart={'1s' min_backoff, '30s' max_backoff, 0.2 random_factor, 10 max_restarts, '1 minute' max_restarts_within}
```

Columns are optional, defaults - min backoff 1s, max backoff 30s, random factor 0.2, unlimited restarts;
`max_restarts_within` defaults to min backoff. Failures are restarted, completion (`to`, `timeout`) is not. After max
restarts the operation fails with the last failure.

- `msg read restart` without `committer` - restarts the reader. It reads from its subscription start again
  (committed offset, specified offset or `auto.offset.reset`), so messages before the failure may be emitted again.
- `msg read restart` with `committer` - restarts reading, processing and committing together after **reader and
  commit failures**. A committable can be committed only by the consumer which has read the message, so the failed
  consumer's messages which are not committed yet cannot be committed any more - a new consumer continues from the
  committed offset, the messages are read and processed again (foreach body, `to`, sending). Processing failures
  (foreach body, `to`, `commit_metadata`, `msg send` parameters, sending) are not restarted - the stream fails.
  The stream is restarted where it is consumed - sent by `msg send`, emitted to http response, collected, folded;
  collected and folded results include messages of failed attempts. Database resources of the operation are the
  same for all attempts, foreach index `__idx` starts from 0 for each attempt. Timeout `within` is total duration
  including restarts - when it expires the stream completes, a failure being restarted at that time is not
  reported (restarts are logged). A backoff in progress is not interrupted, so the stream may complete up to max
  backoff later.
- `msg send restart` - restarts the whole pipeline: operation (`msg read`, foreach), sending and committing, after
  any failure. The operation must return a messaging result and is evaluated again for each restart - a new
  consumer continues from the committed offset; steps of the operation before `msg read` are executed again too.
  Messages sent but not committed before the failure are sent again; the result count includes them.

Which restart to use:

| Consumption | Failure source | Restart |
|---|---|---|
| any | reader (broker connection), committing | `msg read restart` |
| `msg send` / `msg multi send` | reader, processing (foreach body, `to`, `filter`), sending, committing | `msg send restart` / `msg multi send restart` |

```
- foreach msg read group='g' from='orders' committer=true restart=true:   # consume into database
  - transaction:
    - =order[id = :id] {status = :status}
```

`msg read restart` does not restart processing failures - failure of foreach body or sending fails the stream
(action). `msg send restart` evaluates the operation again and rebuilds the whole pipeline, so the operation must
return a messaging result. Both can be specified - reader and commit failures are restarted by `msg read restart`
(until its max restarts), other failures by `msg send restart`.

Message processing (foreach body, per message tresql) left running by the failed stream completes before
processing of the stream restarted by `msg read restart` starts. Its message was not committed, so it is processed
again. `msg send restart` evaluates the operation again, so processing of its operation (foreach body, `to`) left
running by the failed stream may overlap with processing of the restarted stream, on the same connection.

Reader configuration errors (e.g. missing group) fail `msg read` - they are not restarted.

Decoding failure is permanent - it is not restarted, see [decoding errors](#decoding-errors). Other permanent
failures of the reader are repeated until max restarts are exhausted.

## Server sent events

An action returning a messaging result (`msg read`, foreach on `msg read`) responds with server sent events
(`text/event-stream`):

- event data - string as is, other values as json (incoming message as map
  `{topic, partition, offset, timestamp, key, headers, value}`). Events are created with server notifications event
  function `app.server-notifications.event-function`, which can be customized.
- keep-alive (heartbeat) comments every `app.msg-sse-keep-alive` (default 15s), so that idle streams are not closed
  by proxies or idle timeouts,
- headers `Cache-Control: no-cache`, `X-Accel-Buffering: no` (no proxy buffering),
- the consumer is stopped when the stream completes (`to`, `timeout`), fails or the client disconnects - the group
  partitions are released immediately, a new client of the same group continues from the committed offset.

## Admin calls

```
msg topics [<provider>]
msg topic partitions [<provider>] <topic rows>
msg beginning offsets [<provider>] {<topic> topic, <partition> partition} ++ ...
msg end offsets [<provider>] {<topic> topic, <partition> partition} ++ ...
msg committed offsets [<provider>] {<group> group_id, <topic> topic, <partition> partition} ++ ...
```

Calls return rows, sorted by group, topic, partition:

| Call | Result row |
|---|---|
| `msg topics`, `msg topic partitions` | `{topic, partition, leader, replicas, in_sync_replicas}`, nodes as `{id, host, port}`, leader `null` if none |
| `msg beginning offsets`, `msg end offsets` | `{topic, partition, offset}` |
| `msg committed offsets` | `{group_id, topic, partition, offset, metadata}`, partitions without committed offset are omitted |

## Headers

- incoming header value - string if it is valid UTF-8, otherwise bytes (`Array[Byte]`), `null` if absent. The
  conversion is lossless - code expecting binary value can convert a string back with `getBytes(UTF_8)`. In json
  (server sent events, responses) binary values are base64 strings.
- outgoing header value - string (sent as UTF-8), bytes or `null`.

## Parameter evaluation

Operation parameters are tresql expressions, column aliases are names:

- column names are chosen not to be SQL reserved words (`group_id`, `commit_when`); other aliases which are
  reserved words of the database must be quoted, e.g. `{1 "offset"}`,
- scalar tresql (`'avro'`, `true`, `:topic`) or single unaliased column is a shorthand where supported: `from` and
  `to` - topic, `using` - value codec, `committer`, `restart` - boolean,
- unknown column is an error naming operation, parameter and allowed columns, e.g.
  `msg read parameter 'committer': unknown column 'max_bach', allowed: delivery, max_batch, ...`,
- numbers must be whole for integer parameters, durations are strings (`5s`, `100 millis`) or milliseconds,
  timestamps are epoch milliseconds or date / time values (local date time in system time zone).

## Provider API

Providers implement `org.wabase.msg.MsgProvider` (Kafka - `org.wabase.msg.kafka.KafkaProvider`):

```scala
trait MsgProvider {
  def createReader(config: Config, keyDecoder: MsgDecoder = null, valueDecoder: MsgDecoder = null): MsgReader
  def createSender(config: Config): MsgSender     // encoders - of message (OutMessage), else from config
  def createCommitter(config: Config): MsgCommitter
  def admin: MsgAdmin
  def codecs: MsgCodecs
  def close(): Future[Done]
}
trait MsgReader {
  def source(subscription: Subscription): Source[InMessage, Control]
  def atMostOnceSource(subscription: Subscription): Source[InMessage, Control]
  def committableSource(subscription: Subscription): Source[CommittableMessage[InMessage], Control]
  def committableSourceWithMetadata(subscription: Subscription, metadata: InMessage => String)
    : Source[CommittableMessage[InMessage], Control]
}
trait MsgSender {
  // None message is not sent, P is passed through in order (e.g. committable of filtered message)
  def optionalFlow[P]: Flow[(Option[OutMessage], P), (Option[SendResult], P), NotUsed]
  // derived: flow, send, sink, committableSink
}
trait MsgCommitter {
  def flow: Flow[Committable, Done, NotUsed]   // commit failure fails the stream
  // derived: sink
}
trait Control {
  def drainOn(streamCompletion: Future[Any]): Unit
  def stop(): Future[Done]
}
```

- `config` of factory methods contains operation specific settings, provider uses its own configuration as fallback.
  Generic keys: `committer { ... }`, `stop-timeout` (`MsgProvider.StopTimeoutPath`).
- `null` codecs are resolved from configuration (`MsgCodecs`).
- messages: `InMessage`, `OutMessage`, `CommittableMessage[M]` - message (any value) with committable of incoming
  message.
- `Control.stop` - with `drainOn` registered stops emitting, waits for stream completion (pending commits), then
  shuts down the consumer; otherwise shuts down immediately (provider may wait for pending commits, e.g. kafka
  consumer `stop-timeout`).

## Limitations

- `msg send` sends one message per element to one topic - use [multi send](#multi-send---fan-out) for several
  destinations or several messages per element.
- encoders cannot add headers to outgoing messages (headers can be set with `msg send headers=`).
- `msg read` `restart` with `committer` does not restart processing failures - use `msg send` `restart`.

## Future improvements

Proposals, not implemented.

- read side filter (`msg read filter=`) - raw message server sent events filtered per client; send
  [filter](#send-filter) covers sending, foreach `if` covers transformed events.
- encoders adding headers (e.g. schema id of schema registries using headers) - optional trait
  `MsgHeaderEncoder extends MsgEncoder { def encodeWithHeaders(...): (Array[Byte], Seq[Header]) }`,
  checked by providers, existing encoders and providers unchanged.
- startup check - warning if `explicit db` views use `msg read` with per message tresql and evaluator pool is not
  configured (currently the stream fails when the first message is processed).
