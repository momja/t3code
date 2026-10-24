import { expect, it } from "@effect/vitest";
import {
  MessageId,
  NodeId,
  ProjectId,
  ProviderInstanceId,
  ProviderDriverKind,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import type * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as InlineQuestions from "./InlineQuestionService.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as RuntimePolicy from "./RuntimePolicy.ts";

function sourceProjection(): OrchestrationV2ThreadProjection {
  const now = DateTime.makeUnsafe("2026-10-24T00:00:00Z");
  const threadId = ThreadId.make("source");
  const providerThreadId = ProviderThreadId.make("source-provider-thread");
  const instanceId = ProviderInstanceId.make("codex");
  const attemptId = RunAttemptId.make("source-attempt");
  const modelSelection = {
    instanceId,
    model: "test",
    options: [{ id: "reasoningEffort", value: "high" }],
  };
  return {
    thread: {
      id: threadId,
      projectId: ProjectId.make("project"),
      title: "Source",
      createdBy: "user",
      creationSource: "web",
      providerInstanceId: instanceId,
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: "/workspace",
      activeProviderThreadId: providerThreadId,
      lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
      forkedFrom: null,
      createdAt: now,
      updatedAt: now,
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      lastVisitedAt: null,
      deletedAt: null,
    },
    runs: [
      {
        id: RunId.make("source-run"),
        threadId,
        ordinal: 1,
        providerInstanceId: instanceId,
        modelSelection,
        providerThreadId,
        userMessageId: MessageId.make("source-message"),
        rootNodeId: NodeId.make("source-node"),
        activeAttemptId: attemptId,
        status: "completed",
        requestedAt: now,
        startedAt: now,
        completedAt: now,
        checkpointId: null,
        contextHandoffId: null,
      },
    ],
    providerThreads: [
      {
        id: providerThreadId,
        driver: ProviderDriverKind.make("codex"),
        providerInstanceId: instanceId,
        providerSessionId: ProviderSessionId.make("source-session"),
        appThreadId: threadId,
        ownerNodeId: null,
        nativeThreadRef: {
          driver: ProviderDriverKind.make("codex"),
          nativeId: "native-source",
          strength: "strong",
        },
        nativeConversationHeadRef: null,
        status: "idle",
        firstRunOrdinal: 1,
        lastRunOrdinal: 1,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      },
    ],
    providerTurns: [
      {
        id: ProviderTurnId.make("source-turn"),
        providerThreadId,
        nodeId: NodeId.make("source-node"),
        runAttemptId: attemptId,
        nativeTurnRef: {
          driver: ProviderDriverKind.make("codex"),
          nativeId: "native-turn",
          strength: "strong",
        },
        ordinal: 1,
        status: "completed",
        startedAt: now,
        completedAt: now,
      },
    ],
    attempts: [],
    nodes: [],
    subagents: [],
    providerSessions: [],
    runtimeRequests: [],
    messages: [],
    plans: [],
    turnItems: [],
    checkpointScopes: [],
    checkpoints: [],
    contextHandoffs: [],
    contextTransfers: [],
    visibleTurnItems: [],
    updatedAt: now,
  };
}

const harness = (options: { supported?: boolean; failFork?: boolean } = {}) =>
  Effect.gen(function* () {
    const source = sourceProjection();
    const before = structuredClone(source);
    const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event, Cause.Done>();
    const started = yield* Deferred.make<ProviderAdapter.ProviderAdapterV2TurnInput>();
    const closed = yield* Deferred.make<void>();
    const opens: ProviderAdapter.ProviderAdapterV2OpenSessionInput[] = [];
    const forks: ProviderAdapter.ProviderAdapterV2ForkThreadInput[] = [];
    const adapter = {
      getCapabilities: () => Effect.die("unused"),
      planSelectionTransition: () => Effect.die("unused"),
      instanceId: source.thread.modelSelection.instanceId,
      driver: ProviderDriverKind.make("codex"),
      ...(options.supported === false ? {} : { supportsEphemeralFork: true }),
      openSession: (input: ProviderAdapter.ProviderAdapterV2OpenSessionInput) =>
        Effect.gen(function* () {
          opens.push(input);
          yield* Effect.addFinalizer(() => Deferred.succeed(closed, undefined));
          return {
            driver: ProviderDriverKind.make("codex"),
            instanceId: input.modelSelection.instanceId,
            events: Stream.fromEffectRepeat(Queue.take(events)),
            forkThread: (fork: ProviderAdapter.ProviderAdapterV2ForkThreadInput) =>
              Effect.gen(function* () {
                forks.push(fork);
                if (options.failFork)
                  return yield* new ProviderAdapter.ProviderAdapterProtocolError({
                    driver: ProviderDriverKind.make("codex"),
                    detail: "fork failed",
                  });
                return {
                  ...source.providerThreads[0]!,
                  id: ProviderThreadId.make("temporary-provider-thread"),
                  appThreadId: input.threadId,
                };
              }),
            startTurn: (turn: ProviderAdapter.ProviderAdapterV2TurnInput) =>
              Deferred.succeed(started, turn).pipe(Effect.asVoid),
          } as unknown as ProviderAdapter.ProviderAdapterV2SessionRuntime;
        }),
    } as ProviderAdapter.ProviderAdapterV2["Service"];
    const layer = InlineQuestions.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          Layer.mock(ProjectionStore.ProjectionStoreV2)({
            getThreadRecords: () => Effect.succeed(source),
          }),
          Layer.mock(ProviderAdapterRegistry.ProviderAdapterRegistryV2)({
            get: () => Effect.succeed(adapter),
          }),
          RuntimePolicy.layer,
          IdAllocator.layer,
        ),
      ),
    );
    const ask = (selection = "selected passage") =>
      InlineQuestions.InlineQuestionService.pipe(
        Effect.flatMap((service) =>
          service
            .ask({
              threadId: source.thread.id,
              question: "Explain this?",
              selection,
            })
            .pipe(Stream.runCollect),
        ),
        Effect.provide(layer),
      );
    return { source, before, started, closed, opens, forks, events, ask };
  });

it.effect(
  "forks native context, streams only the temporary answer, and leaves the parent unchanged",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const h = yield* harness();
        const fiber = yield* h.ask().pipe(Effect.forkChild);
        const turn = yield* Deferred.await(h.started);
        expect(h.opens[0]?.ephemeral).toBe(true);
        expect(h.opens[0]?.threadId).not.toBe(h.source.thread.id);
        expect(turn.modelSelection).toEqual(h.source.runs[0]!.modelSelection);
        expect(turn.runtimePolicy.runtimeMode).toBe(h.source.thread.runtimeMode);
        expect(h.opens[0]?.ephemeralSourceThreadId).toBe(h.source.thread.id);
        expect(h.forks[0]?.sourceProviderThread).toBe(h.source.providerThreads[0]);
        expect(h.forks[0]?.providerTurnId).toBe(h.source.providerTurns[0]?.id);
        expect(turn.message.text).toContain("selected passage");
        const message = {
          id: MessageId.make("answer"),
          threadId: turn.threadId,
          runId: turn.runId,
          nodeId: turn.rootNodeId,
          role: "assistant" as const,
          text: "First",
          attachments: [],
          streaming: true,
          createdBy: "agent" as const,
          creationSource: "provider" as const,
          createdAt: h.source.updatedAt,
          updatedAt: h.source.updatedAt,
        };
        yield* Queue.offerAll(h.events, [
          {
            type: "message.updated",
            driver: ProviderDriverKind.make("codex"),
            message: { ...message, threadId: h.source.thread.id, text: "parent output" },
          },
          { type: "message.updated", driver: ProviderDriverKind.make("codex"), message },
          {
            type: "message.updated",
            driver: ProviderDriverKind.make("codex"),
            message: { ...message, text: "Final answer", streaming: false },
          },
          {
            type: "turn.terminal",
            driver: ProviderDriverKind.make("codex"),
            providerThreadId: turn.providerThread.id,
            providerTurnId: ProviderTurnId.make("answer-turn"),
            runOrdinal: 1,
            status: "completed",
            failure: null,
            threadDisposition: "reusable",
          },
        ]);
        expect(yield* Fiber.join(fiber)).toEqual([
          { text: "First", status: "answering" },
          { text: "Final answer", status: "answering" },
          { text: "Final answer", status: "completed" },
        ]);
        yield* Deferred.await(h.closed);
        expect(h.source).toEqual(h.before);
      }),
    ),
);

it.effect("closing the popup interrupts the temporary runtime", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const h = yield* harness();
      const fiber = yield* h.ask().pipe(Effect.forkChild);
      yield* Deferred.await(h.started);
      yield* Fiber.interrupt(fiber);
      yield* Deferred.await(h.closed);
      expect(h.source).toEqual(h.before);
    }),
  ),
);

it.effect("refuses providers without an ephemeral fork before opening a session", () =>
  Effect.gen(function* () {
    const h = yield* harness({ supported: false });
    const error = yield* h.ask().pipe(Effect.flip);
    expect(error.message).toContain("Codex and Claude");
    expect(h.opens).toEqual([]);
  }),
);

it.effect("closes the runtime when a fork fails", () =>
  Effect.gen(function* () {
    const h = yield* harness({ failFork: true });
    const error = yield* h.ask().pipe(Effect.flip);
    expect(error.message).toContain("fork failed");
    yield* Deferred.await(h.closed);
  }),
);

it.effect("reports a provider that ends without a terminal event", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const h = yield* harness();
      const fiber = yield* h.ask().pipe(Effect.flip, Effect.forkChild);
      yield* Deferred.await(h.started);
      yield* Queue.end(h.events);
      expect((yield* Fiber.join(fiber)).message).toContain("before answering");
      yield* Deferred.await(h.closed);
    }),
  ),
);
