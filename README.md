# Event Pipeline

A REST service that stores customer events in MongoDB and asynchronously sends event IDs to SQS for
processing. Duplicate requests and deliveries use MongoDB uniqueness and atomic state transitions;
retry, relay and recovery jobs handle failures and interrupted work. The included business processor
is a placeholder with no external side effects.

Stack: Java 17, Spring Boot 3.5, Spring Data MongoDB, AWS SDK v2/SQS, MongoDB 7 and LocalStack 4.14.
Tests use JUnit 5, Mockito, Awaitility, ArchUnit and Testcontainers.

## Prerequisites

* Docker with Compose; Docker must be running for the full test suite.
* Java 17 or a compatible JDK to run tests locally; the compilation target is Java 17.
* No local Maven installation is needed: use `mvnw.cmd` on Windows or `./mvnw` on bash.

Run commands from the repository root. The Compose stack publishes application port 8080, MongoDB
port 27017 and LocalStack port 4566; these ports must be free.

## Run locally

**Important: create a `.env` file before starting.** It is git-ignored because it holds credentials,
so it is not in the repository, and the stack will not start correctly without it. Copy
[.env.example](.env.example) to `.env` using your shell:

| Shell | Command |
| --- | --- |
| Windows CMD | `copy .env.example .env` |
| PowerShell | `Copy-Item .env.example .env` |
| bash | `cp .env.example .env` |

Then fill in the empty MongoDB credential fields and the LocalStack-only AWS credential fields. Use
different root/application MongoDB usernames and generated alphanumeric local passwords to avoid the
current bootstrap script and MongoDB URI escaping limits. Use non-production LocalStack credentials,
not real AWS keys. No credential defaults are supplied.

```text
docker compose up --build -d
```

Wait for application startup, then check health; repeat while it is initializing:

```bash
curl --fail --silent --show-error http://localhost:8080/actuator/health
```

Windows CMD or PowerShell:

```powershell
curl.exe --fail --silent --show-error http://localhost:8080/actuator/health
```

Expect `{"status":"UP","groups":["liveness","readiness"]}`. The SQS queues are created by the
LocalStack ready hook. MongoDB initialization creates the application user; application startup creates
the event collection and validator if the collection is absent, and creates any missing indexes.

Stop and remove the stack:

```text
docker compose down
```

To also remove attached anonymous volumes and orphaned service containers:

```text
docker compose down --volumes --remove-orphans
```

Local data is disposable: MongoDB has no named/host data volume in Compose and LocalStack persistence
is not enabled. A `down` followed by a fresh `up` is not a data-preservation workflow. Keep
`SERVER_PORT=8080` for these commands; changing it also requires changing Compose's fixed port mapping.

## Tests

Run the complete suite with Docker running; the local Compose stack may be stopped:

```powershell
.\mvnw.cmd test
```

From bash, use `./mvnw test`. Tests start their own MongoDB and LocalStack containers and create
queues/redrive policies themselves. They do not read `.env` or use the LocalStack ready hook.
The default command includes every test. `-Pfast-tests` explicitly selects the lightweight tests;
`-Pintegration-tests` is a compatibility alias for the complete default suite.

If Testcontainers reports **"Could not find a valid Docker environment"** together with a Docker
**Status 400** response on a recent Docker Desktop, set `api.version=1.44` in the per-user file
`%USERPROFILE%\.docker-java.properties` (`$HOME/.docker-java.properties` on bash). Preserve any other
entries and rerun the tests. This is a per-machine Docker client compatibility workaround, not a
repository setting; do not commit the file. First ensure Docker Desktop is running and `docker version`
can reach its server.

## API examples

The repository sample below uses a historical client timestamp. Customer queries filter the service's
`createdAt` receive time, so the broad example date range intentionally includes newly received events.
Run against a fresh local stack, or use a new `eventId` if this ID already has a different payload.

bash POST:

```bash
curl -i -X POST http://localhost:8080/api/events \
  -H 'Content-Type: application/json' --data-binary @- <<'JSON'
{
  "eventId": "evt-123",
  "customerId": "cust-1",
  "type": "PAYMENT",
  "amount": 10.00,
  "currency": "USD",
  "timestamp": "2025-01-01T00:00:00Z"
}
JSON
```

Equivalent Windows PowerShell POST:

```powershell
$event = @'
{
  "eventId": "evt-123",
  "customerId": "cust-1",
  "type": "PAYMENT",
  "amount": 10.00,
  "currency": "USD",
  "timestamp": "2025-01-01T00:00:00Z"
}
'@
Invoke-RestMethod -Method Post -Uri 'http://localhost:8080/api/events' `
  -ContentType 'application/json' -Body $event
```

Both return **202 Accepted**, a `Location: /api/events/evt-123` header and a body such as
`{"eventId":"evt-123","status":"RECEIVED","duplicate":false}`. Processing is asynchronous; poll
GET until `status` is `PROCESSED`:

```bash
curl --fail --silent --show-error http://localhost:8080/api/events/evt-123
curl --fail --silent --show-error 'http://localhost:8080/api/customers/cust-1/events?from=2020-01-01T00%3A00%3A00Z&to=2100-01-01T00%3A00%3A00Z&status=PROCESSED&page=0&size=20'
```

Equivalent Windows PowerShell requests:

```powershell
Invoke-RestMethod -Uri 'http://localhost:8080/api/events/evt-123'
Invoke-RestMethod -Uri 'http://localhost:8080/api/customers/cust-1/events?from=2020-01-01T00%3A00%3A00Z&to=2100-01-01T00%3A00%3A00Z&status=PROCESSED&page=0&size=20'
```

GET endpoints return **200 OK**. The list body contains `content`, `page`, `size`, `totalElements` and
`totalPages`; ordering is `createdAt` descending. `from` is inclusive and `to` exclusive; all filters
are optional, page starts at 0, and size defaults to 20 with allowed range 1–100. Single-event/list
items expose `eventId`, `customerId`, `type`, `amount`, `currency`, `status`, `createdAt` and `processedAt`.

An identical POST replay returns **202**, `duplicate:true`, the current stored status and
`Idempotent-Replayed:true`; it does not publish another initial message. Reusing an ID with different
payload fields returns **409 Conflict**. Missing events return **404**. Invalid input/query parameters
return **400**; storage data-access/connectivity failures return **503** with `Retry-After: 5`.

Errors use `application/problem+json`, with `type`, `title`, `status`, `detail` and `instance`. Validation
field errors additionally have an `errors` array. For example:

```json
{
  "type": "urn:problem:event-not-found",
  "title": "Not Found",
  "status": 404,
  "detail": "Event with eventId 'missing' was not found",
  "instance": "/api/events/missing"
}
```

Defined error types are `urn:problem:validation-failed` (400), `urn:problem:event-not-found` (404),
`urn:problem:event-payload-conflict` (409), `urn:problem:service-unavailable` (503) and
`urn:problem:internal-error` (500). The advice's **405** response uses `about:blank`.
POST requires nonblank IDs (maximum 128 characters), a `type` of `PAYMENT` or `REFUND` (any other value
returns 400), a positive amount (up to 16 integer and
6 fractional digits), three uppercase currency letters and an ISO-8601 timestamp. Optional metadata
allows at most 20 entries, nonempty keys up to 50 characters and values up to 200 characters.

## Configuration

All names below appear in `.env.example`, including commented optional overrides. Defaults are
non-secret; credential fields have no example values. Compose passes environment settings to the app.

| Variable | Meaning / example default | Secret? |
| --- | --- | --- |
| `APP_NAME` | Application/log name; `event-pipeline` | No |
| `SERVER_PORT` | Application listen port; `8080` (Compose mapping is fixed) | No |
| `MONGO_HOST` | MongoDB host; `mongodb` in Compose | No |
| `MONGO_PORT` | MongoDB port; `27017` | No |
| `MONGO_DATABASE` | Database name; `event_pipeline` | No |
| `MONGO_ROOT_USERNAME` | MongoDB bootstrap user identifier; fill locally | No (credential ID) |
| `MONGO_ROOT_PASSWORD` | MongoDB bootstrap authentication; fill locally | Yes |
| `MONGO_USERNAME` | Application user identifier; fill locally | No (credential ID) |
| `MONGO_PASSWORD` | Application authentication; fill locally | Yes |
| `AWS_REGION` | AWS/SQS region; `us-east-1` | No |
| `AWS_SQS_ENDPOINT` | Local override; `http://localstack:4566`; omit for AWS | No |
| `AWS_SQS_QUEUE_NAME` | Main queue; `event-pipeline-events` | No |
| `AWS_SQS_DLQ_NAME` | Dead-letter queue; `event-pipeline-events-dlq` | No |
| `AWS_ACCESS_KEY_ID` | LocalStack credential identifier; fill locally | No (credential ID) |
| `AWS_SECRET_ACCESS_KEY` | LocalStack authentication; fill locally | Yes |
| `AWS_SQS_CONSUMER_ENABLED` | Enable the `@SqsListener` consumer; `true` | No |
| `AWS_SQS_CONSUMER_MAX_MESSAGES` | Messages requested per poll (capped at the worker count); `10` | No |
| `AWS_SQS_CONSUMER_WAIT_TIME_SECONDS` | Long-poll wait; `20` | No |
| `AWS_SQS_CONSUMER_VISIBILITY_TIMEOUT_SECONDS` | Initial received-message invisibility; `30` | No |
| `AWS_SQS_CONSUMER_WORKER_THREADS` | Max messages processed concurrently; `4` | No |
| `AWS_SQS_CONSUMER_SHUTDOWN_WAIT_SECONDS` | In-flight message shutdown deadline; `25` | No |
| `PROCESSING_MAX_ATTEMPTS` | Processing claim limit; `3`; must equal main-queue redrive `maxReceiveCount` | No |
| `RETRY_BACKOFF_BASE_SECONDS` | Retry visibility backoff base; `5` | No |
| `RETRY_BACKOFF_CAP_SECONDS` | Retry visibility backoff cap; `60` | No |
| `RELAY_ENABLED` | Enable stale-RECEIVED relay; `true` | No |
| `RELAY_INTERVAL_SECONDS` | Fixed delay between relay runs; `10` | No |
| `RELAY_STALE_RECEIVED_THRESHOLD_SECONDS` | RECEIVED lease eligibility age; `60` | No |
| `RELAY_BATCH_SIZE` | Maximum leases per relay run; `100` | No |
| `RECOVERY_ENABLED` | Enable stale-PROCESSING recovery; `true` | No |
| `RECOVERY_INTERVAL_SECONDS` | Fixed delay between recovery runs; `10` | No |
| `RECOVERY_STALE_PROCESSING_THRESHOLD_SECONDS` | Stale claim age; `300`; must exceed visibility timeout and maximum expected processing time | No |

The AWS SDK already uses its default credentials provider chain. **Proposed for AWS:** supply an IAM
task role instead of static keys and obtain real secrets from Secrets Manager or SSM Parameter Store;
this repository does not provision those services.

## Architectural decisions

* MongoDB is the durable event record; SQS messages contain only `eventId` plus a schema-version attribute.
* Unique `_id=eventId` and insert-first ingestion arbitrate concurrent duplicate requests atomically.
* The publisher sends asynchronously (Spring Cloud AWS `SqsTemplate`), so a slow or failing send never
  affects the HTTP response; the relay re-publishes anything that did not reach SQS.
* The consumer is a Spring Cloud AWS `@SqsListener` with manual acknowledgement: a message is deleted
  only after successful processing.
* A standard SQS queue provides at-least-once delivery; atomic claims and completion handle duplicates.
* Claim attempts fence worker transitions so stale workers cannot overwrite newer results.
* Relay/recovery use MongoDB leases and state changes; ingestion and processing remain independent,
  enforced by ArchUnit.
* Strict BSON validation, Decimal128 amounts and indexes protect storage types and support lookups.

See [ARCHITECTURE.md](ARCHITECTURE.md) for diagrams, failure cases, scaling trade-offs and clearly
labelled production proposals.

## Known limitations

Business effects must be idempotent: a crash or processing longer than the stale threshold can execute
them again despite fenced state updates. Recovery adds the relay's stale window before republishing;
DLQ copies require operational inspection/cleanup, and FAILED replay is a manual MongoDB action.
The business processor is a no-op, the API has no authentication, and the local data setup is disposable.
Startup requires MongoDB and the SQS queues. Only health is exposed through Actuator; production
dashboards, alarms, tracing, autoscaling and AWS deployment are proposals, not repository features.
