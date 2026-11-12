# AI usage

## Tools

* **Claude Code** (Anthropic CLI / VS Code extension), used interactively inside the repository so it
  could read the code, edit files and run the Maven build and tests.

## Workflow

I split the assignment into 6 tasks and wrote a separate prompt for each one, using Claude for
every task. Working task by task kept each prompt small and made each diff easy to review before
moving on.

## What the AI helped with

The AI's first implementation was often not the right one. In each area below I reviewed it, saw
it was not the correct way, and asked the AI to change the implementation.

| Area | AI's first implementation (not correct) | What I asked the AI to change |
| --- | --- | --- |
| Refactoring | Long positional constructors, request-to-command copying inline, and payload comparison spread across classes. | Builders (`EventCreationCommand`, `Event`), an `EventRequestMapper` for the request-to-command copy, and the duplicate-payload comparison moved onto `EventCreationCommand.matches(Event)`. |
| Consumer | A hand-written `SmartLifecycle` long-poll loop. | A Spring Cloud AWS `@SqsListener` (manual acknowledgement, visibility backoff kept). |
| Publisher | Async publishing through its own bounded executor, a separate sync client and queue-full handling. | A non-blocking `SqsTemplate` send (`sendAsync`); failed sends are only logged because the relay job re-publishes `RECEIVED` events. |
| Design | Extra interfaces, custom helper classes instead of standard ones, and passwords written in the code. | Fewer interfaces, standard classes with no new dependencies, and credentials moved to configuration. |
| Cleanup | Unused configuration and tests left in place; docs out of sync with the code. | Removed the unused parts and aligned README / ARCHITECTURE.md / `.env.example` / Compose with the code. |

## Accepted and rejected suggestions

* **Rejected: MapStruct for the request mapper.** The project had no MapStruct and the controller
  tests construct `EventController` directly, so a hand-written static mapper avoided a new
  dependency and a constructor change.
* **Rejected: a mapper for `Event` creation.** Creating an `Event` adds server-side values (status,
  clock-based timestamps, attempts), so a builder at the call site keeps those rules visible.
* **Rejected, then reversed: `@Async` for publishing.** The AI first advised against it because a
  rejected task would throw after the Mongo insert and turn a stored event into an HTTP 500. After
  I asked for a simpler design, we removed the executor altogether and used the SDK's non-blocking
  client through `SqsTemplate`, which has no rejection path.
* **Accepted: `@SqsListener` instead of the custom poller**, after confirming it supports manual
  acknowledgement and visibility changes, which the failure-handling design depends on.
* **Accepted:** aligning the AWS SDK version with the Spring Cloud AWS BOM (2.31.78) to avoid mixed
  AWS SDK jars.

* **Rejected: extra dependencies and non-standard classes.** I asked the AI not to add
  dependencies, yet it sometimes produced custom helper classes that duplicated what the standard
  library or the existing stack already provides. I asked for the standard classes instead.
* **Rejected: too many interfaces.** The AI added an interface for almost every class, even with a
  single implementation. I removed them to keep the design simple.
* **Rejected: hard-coded passwords.** Several times the AI put passwords and credentials directly in
  the code. I moved them to configuration and environment variables (`.env.example`) and now check
  each diff for secrets before keeping it.
* **Accepted: one `ApiException` with factory methods.** The AI first created an abstract base class
  with one subclass per status (`EventNotFoundException`, `EventConflictException`,
  `InvalidRequestException`). I asked for a single class with `notFound`, `conflict` and `badRequest`
  factories, because the subclasses differed only by status and type.
* **Accepted: removing redundant checks from `CurrencyValidator`.** The AI added an uppercase check and
  a length check around `Currency.getInstance`, which already rejects those inputs. I checked the
  behaviour in `jshell` and asked for the validator to rely on the lookup alone.
* **Accepted: a shorter `Location` builder.** The AI built the `Location` header with a multi-step
  builder. I asked for a URI template with `.encode()` and `buildAndExpand`, which encodes the event ID
  and keeps the same header value. The tests that assert the header still pass.
* **Accepted: jittered SQS retry delay.** The AI's backoff was deterministic, so messages that failed
  together, for example during a Mongo outage, would all become visible again at the same time. I asked
  for "equal jitter" (between half and all of the exponential ceiling). The random source is injected,
  so the tests can check the bounds exactly.

* **Accepted: merging `EventProcessor` and `DefaultEventProcessor`.** I asked whether the processing
  design was too complex. The AI had an interface with a single implementation, so we merged them into
  one concrete `@Component`. The tests that used lambdas as the processor now use a mock.
* **Accepted: removing the delegating `process(String eventId)` overload.** It only forwarded to the
  other method with the value "unavailable". I told the AI to remove it.
* **Accepted: simplifying "lost ownership".** The AI explained that a slow worker's update matches
  nothing when recovery has handed the event to another worker, and that this is what protects against
  overwriting newer state. It then offered to drop the separate `LOST_OWNERSHIP` outcome, because the
  listener treated it like `IN_PROGRESS_ELSEWHERE`. I left that choice to the AI. The check stays, and
  `handleLostClaim` now returns `IN_PROGRESS_ELSEWHERE`.
* **Accepted: parameterized tests for `RetryBackoff`.** After adding jitter, the AI wrote 23 separate
  tests. I asked for `@ParameterizedTest` so the ceiling cases are a table, and the jitter-bound tests
  use an injected random function.
* **Accepted: splitting long methods without changing behavior.** I found the failure-handling catch in
  `EventProcessingService` hard to read, so it moved into `handleBusinessFailure`. `FailureReason.from`
  was split into `isSensitive` and `redactEventValues`. The A/D comments and the log output stayed the
  same.
* **Used AI to read code before deleting it.** Before removing anything, I asked whether a class was in
  use. `StuckProcessingRecovery` has no direct callers, but it is a scheduled, conditionally enabled
  component, and without it a crashed worker would leave events in PROCESSING forever. It stayed.

## Limits and remaining risks

* The business step must be idempotent. A live worker slower than the stale-PROCESSING threshold can
  run it twice. The `attempts` fencing token only prevents the slower worker from overwriting a newer
  result.
* `maxAttempts` = `maxReceiveCount` is not checked at startup, because that needs an extra SQS call.
* FAILED events are terminal. Someone has to inspect them and reset the state to reprocess.

## How AI output was verified

* Ran the full suite with `./mvnw test`. It uses real MongoDB and LocalStack containers
  (Testcontainers), including retry, DLQ redrive, crash-recovery and duplicate-delivery tests.
* Ran the ArchUnit test that keeps the ingestion and processing sides independent.
* Read the library sources (container defaults, header names, `Visibility`, `SqsTemplate` options)
  instead of relying on remembered APIs.
* Reviewed every diff before keeping it, and kept the existing integration tests as the
  behaviour contract during each refactor.

## Incorrect or misleading AI output that was caught

* The first `@SqsListener` container configuration passed `maxMessagesPerPoll=10` with
  `maxConcurrentMessages=1`. The library rejects that combination at startup; the integration tests
  failed on context start. The fix caps the poll size at the concurrency limit.
* After the publisher simplification, the test base class tracked in-flight sends through a Spring
  spy bean. `SqsRedriveIntegrationTest` has no Spring context, so the field was null and the test
  failed with `NullInsteadOfMock`. The full run caught it and the tracking now skips that case.
* The AI created the `events` indexes only when it created the collection. Collections that already
  existed never got the `customerId` or `status`/`updatedAt` indexes, so recovery and relay would
  scan the whole collection. I asked for the indexes to be ensured on every startup. The next
  version then called `createIndex` every time, which can be slow on a large collection and needs
  index-creation rights. I asked for a check first, so `EventCollectionInitializer` lists the
  existing indexes and creates only the missing ones.
* The first `GlobalExceptionHandler` had a catch-all `Exception` handler that also caught Spring's own
  404 for unknown routes, so those requests returned 500. I asked for the class to extend
  `ResponseEntityExceptionHandler`, so standard MVC errors keep their status and headers, and added
  integration tests for an unknown route (404) and an unsupported method (405 with an `Allow` header).
* **Caught: too many properties, duplicated across files.** The AI created a large set of
  configuration properties and wrote each one into several places: `application.yml` (with its
  default), `docker-compose.yml` (with the same default again) and `.env.example`. Changing a default
  meant editing several files, and they could drift apart. I asked for the duplication to be removed,
  so the defaults live in `application.yml` and Compose passes `.env` through.
* **Caught: a removed default that would break real AWS.** While trimming the configuration, the AI
  removed the empty default from `AWS_SQS_ENDPOINT`. Against real AWS, where the variable is absent by
  design, the application would fail to start. I restored the default (`${AWS_SQS_ENDPOINT:}`).
* **Accepted: a startup check for a cross-setting rule.** Several settings depend on each other, but
  the AI left the rule in comments only. I asked for `PipelineSettingsValidator`, which stops startup
  when the stale-PROCESSING threshold is not greater than the SQS visibility timeout. The
  `maxAttempts` = `maxReceiveCount` rule is still not checked, because that needs an extra SQS call.
