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
  - [Send filter](#send-filter)
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

`using` parameter - tresql returning codec names, evaluated once per operation:

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

Message processing runs after the action has completed, without action's database connection (see
[database resources](#database-resources)). Tresql evaluated per message (`to`, `commit_metadata`, foreach body) uses
connection from evaluator pool `app.wabase.evaluator.pool` (default `_evaluator_`), which must be configured in
`jdbc.cp`, otherwise the stream fails with `Unable to find connection pool`. A connection is borrowed for each
evaluation - size the pool accordingly.

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
- `__idx` - message index (foreach body only),
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

## Database resources

A messaging result is consumed after the action has completed: action's database connection is released (without
`explicit db: true` - action's transaction is committed) before streaming starts. Therefore:

- parameters evaluated once (`from`, `group`, `committer`, ...) are evaluated while the action runs - with action's
  connection and may access action's database,
- tresql evaluated per message (`to`, `commit_metadata`, foreach body) runs on [evaluator
  connection](#evaluator-connection-pool) and can access only tables of evaluator database,
- foreach body accessing the database opens its own `db` / `transaction` - the connection is held only while the
  message is processed, and the message transaction is committed before the message offset:

```
- foreach msg read group='g' from='orders' committer=true:
  - transaction:
    - =order[id = :id] {status = :status}
```

A continuous stream holds no database connection. `explicit db: true` is recommended for views using `msg read` -
only to avoid opening an action connection which is not needed.

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

See [view actions](view-actions.md#msg-send) for parameter syntax. Parameters are evaluated before the operation,
except `filter`. Operation result is sent:

- messaging result (`msg read`, foreach on `msg read`), stream, rows, collection - each element as a message,
- single value (map, scalar) - one message,
- `null` elements and no result - nothing is sent.

Incoming message (`msg read` without foreach) is forwarded - its value is sent, its key is kept unless `key` is
specified. Committing reads are committed after messages are sent, in offset order.

`key` - `name` takes the key from a field of map value (`should_remove` removes the field from the value), `value`
is the key itself. Key is encoded with key codec - `string` by default, use for example `using={'json' key}` for map
keys. Header values must be strings, bytes or null, other values are converted to string.

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
- evaluated on [evaluator connection](#evaluator-connection-pool), also for non messaging results (rows are read
  from action's connection meanwhile), so the evaluator pool must be configured.

Result is sent message count. The operation completes when all messages are sent and committed - with an infinite
stream never (e.g. a job forwarding messages).

Query result rows are sent as they are read from the cursor, they are not collected into memory. For large results:
- `tresql.max-result-size` (default 10000, `0` - unlimited) applies - exceeding it fails the send with
  `TooManyRowsException` after rows sent so far, raise it for the database (`tresql.<db>.max-result-size`),
- JDBC driver may still fetch all rows at once - for example PostgreSQL fetches rows in batches only with autocommit
  off and `tresql.fetch-size` (`tresql.<db>.fetch-size`) set.

## Restart on failure

```
restart=true                                          # defaults
restart={'1s' min_backoff, '30s' max_backoff, 0.2 random_factor, 10 max_restarts, '1 minute' max_restarts_within}
```

Columns are optional, defaults - min backoff 1s, max backoff 30s, random factor 0.2, unlimited restarts;
`max_restarts_within` defaults to min backoff. Failures are restarted, completion (`to`, `timeout`) is not. After max
restarts the operation fails with the last failure.

- `msg read restart` - restarts the reader. It reads from its subscription start again (committed offset, specified
  offset or `auto.offset.reset`), so messages before the failure may be emitted again. Not supported with
  `committer` - committables of the failed consumer cannot be committed.
- `msg send restart` - restarts the whole pipeline: operation (`msg read`, foreach), sending and committing. Use it for
  committing reads. The operation must return a messaging result and is evaluated again for each restart - a new
  consumer continues from the committed offset; steps of the operation before `msg read` are executed again too.
  Messages sent but not committed before the failure are sent again; the result count includes them.

A permanent failure (e.g. message which cannot be decoded) is repeated until max restarts are exhausted.

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
  def createSender(config: Config, keyEncoder: MsgEncoder = null, valueEncoder: MsgEncoder = null): MsgSender
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

- one outgoing message per incoming message, one destination topic per `msg send` - fan-out is not supported
  (see [future improvements](#future-improvements)). To send messages of one topic to several destinations, use
  several `msg send` operations (e.g. jobs), each reading the topic with its own consumer group.
- encoders cannot add headers to outgoing messages (headers can be set with `msg send headers=`).
- `msg read` `restart` cannot be combined with `committer` - use `msg send` `restart`.

## Future improvements

Proposals, not implemented.

### Multi send - fan-out

Sends each element of the source operation to several destinations, routing (destination filter), copying and
per destination transformation (including splitting):

```
- msg multi send restart=true:
  - msg read group='router' from='orders' committer=true
  to:
  - msg send to='orders-eu' key={'id' name}:                          # adjusted per destination
    - if {:region = 'eu'} (unique {:id id, :amount amount, 'EUR' currency})
  - msg send to='orders-us' filter={:region != 'eu'} (unique {:id id, :amount amount})
  - msg send [other] to='audit'                                       # element as is, other provider
- db use:                                                             # source and destinations share connection
  - msg multi send:
    - foreach audit_log[time > :since]{id, action, user_id}:          # any source - db rows, http, foreach
      - unique {:id id, :action action, :user_id user_id}
    to:
    - msg send to='audit' key={'id' name}
    - msg send to='user_events' filter={:user_id != null}
```

Syntax - step with two keys like `if` / `else`: `msg multi send [restart=...]` with source operation (block), and `to`
with list of destination `msg send` operations.

Source - any operation, as `msg send` operation: messaging result (`msg read`, foreach on it), stream, rows,
collection, single value. Outer foreach is optional - only for processing common to all destinations (e.g. db
write done once).

Destination - `msg send` with its parameters (provider, `to`, `key`, `headers`, `using`, `filter`), evaluated once
before the source, and optional operation:
- operation is body of implicit foreach over source elements - scope is element scope (message bind variables for
  incoming message, foreach item scope for other elements, `__idx`, parent with `..`); without operation the
  element is sent as is,
- operation result `null` or no result - nothing is sent to the destination (filter); rows - each row is a message
  (splitting),
- incoming message key is retained unless `key` is specified, with or without operation,
- `restart` is not allowed - restart is specified on `msg multi send`.

Delivery:
- for each element destinations are evaluated in order (filters and operations), then messages are sent; committable
  is committed after the element is sent to all destinations (filtered ones included) - at least once across
  destinations, after restart a destination may receive a message again. Side effects of destination operations
  (db writes) are at least once too.
- `restart` - only with messaging source, restarts source, processing, sending and committing, as `msg send restart`.
- non messaging source (db rows, http) - sending is not tied to the source, after failure messages may be already
  sent to some destinations; mark rows as sent in the same transaction or use transactional outbox with messaging
  source.

Database resources:
- messaging source - stream runs after action has completed, destination operations use evaluator pool, as foreach
  body on messaging result,
- other sources - action waits for the send (result is returned after the stream completes), destination operations
  run with action's resources, like foreach body. Wrap multi send in `db use` / `transaction` so that source and
  destination operations use the same database (resources of `db` before the source only cover the source).

Implementation - one stage per destination `sender.optionalFlow[(element, P)]` (`None` - filtered, element with
several messages - several sends, `P` passed after the last), stages are chained, `P` (committable) after the last
stage goes to committer. Works across providers without provider API change, order per destination is preserved,
stages process different elements concurrently. Filters and operations of all destinations of an element are
evaluated in one future with one evaluator connection. Kafka `ProducerMessage.multi` within one provider - optional
optimisation.

Result - rows `{provider, topic, count}` of sent message counts, one row per destination in destination order
(several destinations may have the same topic), filtered elements are not counted.

Destination `to` is evaluated once, before the source, as `msg send` `to` - routing is done with destination
filters or operations among destinations known in advance. Topic evaluated per element (e.g. topic per tenant) is
not supported, possible extension if needed.

Name `to:` continues the step like `else` of `if` - "multi send source to destinations", meaning destination as
`msg send to=` (`msg read to=` - termination - is a parameter of other operation, not a block key).

### Other

- read side filter (`msg read filter=`) - raw message server sent events filtered per client; send
  [filter](#send-filter) covers sending, foreach `if` covers transformed events.
- encoders adding headers (e.g. schema id of schema registries using headers) - optional trait
  `MsgHeaderEncoder extends MsgEncoder { def encodeWithHeaders(...): (Array[Byte], Seq[Header]) }`,
  checked by providers, existing encoders and providers unchanged.
- startup check - warning if views use `msg read` with per message tresql and evaluator pool is not configured
  (currently the stream fails when the first message is processed).
