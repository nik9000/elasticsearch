# Migrate ESQL Away from Thread-Local Warnings

## Current State

There are **two categories** of `HeaderWarning.addWarning()` call sites inside ESQL:

### A. Compute-layer (hot path, multi-threaded)

`Warnings.java` (`compute/src/main/java/.../operator/Warnings.java`) calls
`HeaderWarning.addWarning()` in `registerException()`. Every generated evaluator
(hundreds of them, e.g. `AddIntsEvaluator`) creates a `Warnings` instance via
`Warnings.createWarnings(driverContext.warningsMode(), source)` and calls
`warnings.registerException(e)`. These run on many threads concurrently.

### B. Planning-layer (single-threaded, 5 call sites)

| File | Warning |
|---|---|
| `Analyzer.java` | "No limit defined, adding default limit of [{}]" |
| `LogicalPlanBuilder.java` | "INLINESTATS is deprecated, use INLINE STATS instead" |
| `WarnLostSortOrder.java` | "SORT is followed by a LOOKUP JOIN..." |
| `RemoveStatsOverride.java` | "Field '{}' shadowed by field at line..." |
| `EsPhysicalOperationProviders.java` | "Field [{}] cannot be retrieved..." |

### How warnings reach the user today

1. All call sites push into `HeaderWarning.addWarning()` which writes to the
   thread-local `ThreadContext` response headers.
2. `DefaultRestChannel.sendResponse()` (line ~159) calls
   `threadContext.getResponseHeaders()` and copies them into the HTTP response
   headers. This is generic infrastructure, not ESQL-specific.
3. For async queries, `AsyncTaskManagementService.storeResults()` captures
   `threadPool.getThreadContext().getResponseHeaders()` and persists them with
   the stored result.
4. Neither `Result` nor `EsqlQueryResponse` carries warnings as a field -- they
   ride entirely on the thread-local.

---

## Migration Plan

### Phase 1: Migrate compute-layer `Warnings`

This is the highest-value change because compute warnings are the most numerous
and the ones most affected by thread-hopping.

**Add `@Nullable List<String> warnings` to `DriverContext`.** Each driver is
single-threaded, so a plain `ArrayList` is sufficient -- no concurrent collection
needed. Add a comment on the field: "Mutable list shared with all evaluators in
this driver. Evaluators append to this list directly." This replaces
`DriverContext.WarningsMode`: a non-null list means "collect warnings" (was
`COLLECT`), a null list means "ignore warnings" (was `IGNORE`). Remove the
`WarningsMode` enum entirely. Rename `warningsMode()` to `warnings()` returning
`@Nullable List<String>`.

When `DriverContext.finish()` is called, the warnings list is captured into
`DriverContext.Snapshot`. Add a method on `Snapshot` --
`dumpWarningsToThreadContext()` -- that iterates the list and calls
`HeaderWarning.addWarning()` for each entry. This fits naturally: `Snapshot` is
already the mechanism for extracting state from a finished driver.

**Pass the `@Nullable List<String>` into `Warnings`.** Change
`Warnings.createWarnings(warningsMode, source)` to
`Warnings.createWarnings(warnings, source)` where `warnings` is the
`@Nullable List<String>`. If null, return `NOOP_WARNINGS`. Otherwise construct a
`Warnings` that appends to the list instead of calling
`HeaderWarning.addWarning()`.

1. **`Warnings` writes to the list.** `registerException()` formats the warning
   string exactly as before, then appends it to the list instead of calling
   `HeaderWarning.addWarning()`.

2. **After each driver finishes**, the driver runner calls
   `snapshot.dumpWarningsToThreadContext()`. This preserves current behavior
   during the transition period -- warnings still end up in the thread context
   -- but production of warnings is now decoupled from the thread-local.

**Generated evaluators need a minor regeneration.** They currently call
`Warnings.createWarnings(driverContext.warningsMode(), source)` which becomes
`Warnings.createWarnings(driverContext.warnings(), source)`. The code generator
(`EvaluatorImplementer`) needs a one-line update; then regenerate all evaluators.

**Other callers of `WarningsMode` to update:**

| File | Change |
|---|---|
| `SingleValueQuery.java` | Pass a `new ArrayList<>()` instead of `WarningsMode.COLLECT` |
| `AbstractLookupService.java` | Same |
| `LookupExecutionPlanner.java` | Same |
| `BlockLoaderWarnings.java` | Store `@Nullable List<String>` instead of `WarningsMode` |
| `Length.java`, `ByteLength.java` | Pass a `new ArrayList<>()` instead of `WarningsMode.COLLECT` |
| Tests (`WarningsTests`, `SingleValueMatchQueryTests`, etc.) | Pass list or null |

### Phase 2: Wire up the "last second" emission

**Add `List<String> warnings` to `Result`** -- the session-level result record.

**Add `List<String> warnings` to `EsqlQueryResponse`** -- gated behind a new
`TransportVersion`. Update `writeTo`/`deserialize` to serialize the list. Update
`equals`/`hashCode`.

**Collect per-driver warnings into `Result.warnings`.** After all drivers
complete, aggregate warnings from every `DriverContext.Snapshot` into the
`Result`.

**In `TransportEsqlQueryAction.toResponse()`**, copy `Result.warnings` into
the `EsqlQueryResponse`.

In `TransportEsqlQueryAction.innerExecute()`, right before handing the response
to the listener, iterate `response.warnings()` and call
`HeaderWarning.addWarning()` for each. This is the single point where ESQL
warnings re-enter the thread context.

For the async path in `AsyncTaskManagementService.storeResults()`, the warnings
are already serialized inside the response, so when the GET async query path
deserializes and returns the response, the same emission logic applies.

**Once the emission point is verified working**, remove the
`snapshot.dumpWarningsToThreadContext()` calls from the driver runner. Warnings
now flow exclusively through the response object.

### Phase 3: Migrate planning-layer warnings (5 call sites)

These are all single-threaded, so each is a small, independent change. Thread a
warnings list through the existing context objects:

| Call site | How to thread the list |
|---|---|
| **`Analyzer.java`** | Add a warnings list to `AnalyzerContext`. The analyzer already receives a context object. |
| **`LogicalPlanBuilder.java`** | Add a warnings list to the parser's context/params (it already has access to configuration). |
| **`WarnLostSortOrder.java`** | Add a warnings list to `LogicalOptimizerContext`, which is already passed to optimizer rules. |
| **`RemoveStatsOverride.java`** | Same as above -- uses `LogicalOptimizerContext`. |
| **`EsPhysicalOperationProviders.java`** | Add a warnings list to the physical operation provider's constructor or context. |

For each, replace `HeaderWarning.addWarning(...)` with appending the formatted
string to the list. After each phase completes (analysis, optimization,
planning), any accumulated warnings flow into the `Result` and then into
`EsqlQueryResponse`.

**Do these one at a time** so each is a small, reviewable PR.

### Phase 4: Cleanup and validation

1. **Update `CsvTests`**: stop reading warnings from
   `threadPool.getThreadContext().getResponseHeaders()` and instead read them
   from `Result.warnings()`. Remove the
   `HeaderWarning.setThreadContext()`/`removeThreadContext()` calls from
   `@Before`/`@After`.

2. **Update integration tests**: any YAML REST tests or integration tests that
   assert on `Warning` response headers should continue to work because Phase 2
   still emits them as HTTP headers. No changes needed to those tests.

3. **Add a build-time or test-time check** that no ESQL production code imports
   `HeaderWarning` (only the single emission point in
   `TransportEsqlQueryAction` should remain).

4. **Remove `import static org.elasticsearch.common.logging.HeaderWarning.addWarning`**
   from all migrated files.

---

## Execution Order and PR Strategy

| PR | Phase | Risk | Size |
|---|---|---|---|
| 1 | Phase 1: Replace `WarningsMode` with `@Nullable List<String>` on `DriverContext`, pass into `Warnings`, update callers, regenerate evaluators, dump to thread context from `Snapshot` | Medium (behavioral change in hot path, touches many files but mechanically) | Large |
| 2 | Phase 2: Add `Result.warnings`, `EsqlQueryResponse.warnings`, aggregate from snapshots, emission point, remove dump calls | Low (plumbing, then cutover) | Medium |
| 3-7 | Phase 3: One PR per planning call site (5 PRs) | Low each | Small |
| 8 | Phase 4: Test cleanup + no-import guard | Low | Small |

PRs 3-7 can be done in parallel after PR 2 merges. The total migration is ~8
PRs, each independently shippable and testable.

## Key Constraints

- Nothing outside ESQL is modified (no changes to `HeaderWarning`,
  `ThreadContext`, `DefaultRestChannel`, or `AsyncTaskManagementService`).
- Warnings still appear as HTTP response headers to callers -- the "last second"
  injection in Phase 2 preserves the external contract.
- The async query storage path continues to work because warnings are now
  serialized inside `EsqlQueryResponse`, and the emission point handles both
  sync and async paths.
