import {
  InlineQuestionError,
  InlineQuestionInput,
  type InlineQuestionEvent,
  latestProviderTurnForAttempt,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as RuntimePolicy from "./RuntimePolicy.ts";
import * as ServerSettings from "../serverSettings.ts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";

export class InlineQuestionService extends Context.Service<
  InlineQuestionService,
  {
    readonly ask: (
      input: InlineQuestionInput,
    ) => Stream.Stream<InlineQuestionEvent, InlineQuestionError>;
  }
>()("t3/orchestration-v2/InlineQuestionService") {}

const make = Effect.gen(function* () {
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const adapters = yield* ProviderAdapterRegistry.ProviderAdapterRegistryV2;
  const policies = yield* RuntimePolicy.RuntimePolicyV2;
  const ids = yield* IdAllocator.IdAllocatorV2;
  const settings = yield* Effect.serviceOption(ServerSettings.ServerSettingsService);

  return InlineQuestionService.of({
    ask: (input) =>
      Stream.unwrap(
        Effect.gen(function* () {
          const question = yield* Schema.decodeUnknownEffect(InlineQuestionInput)(input);
          const source = yield* projections.getThreadRecords(question.threadId, [
            "runs",
            "providerThreads",
            "providerTurns",
          ]);
          if (source.thread.deletedAt !== null) {
            return yield* new InlineQuestionError({ message: "This thread has been deleted." });
          }
          const sourceProviderThread = source.providerThreads.find(
            (thread) => thread.id === source.thread.activeProviderThreadId,
          );
          const sourceRun = source.runs
            .filter(
              (run) =>
                run.providerThreadId === sourceProviderThread?.id &&
                (run.status === "completed" || run.status === "waiting"),
            )
            .sort((a, b) => b.ordinal - a.ordinal)[0];
          const sourceTurn = latestProviderTurnForAttempt(
            source.providerTurns,
            sourceRun?.activeAttemptId,
          );
          if (
            !sourceProviderThread ||
            !sourceRun ||
            !sourceTurn ||
            sourceProviderThread.nativeThreadRef?.strength !== "strong"
          ) {
            return yield* new InlineQuestionError({
              message: "Ask is available after this session has completed a response.",
            });
          }
          const adapter = yield* adapters.get(sourceRun.providerInstanceId);
          if (Option.isSome(settings)) {
            const effective = resolveProjectSettings(
              yield* settings.value.getSettings,
              source.thread.projectId,
            ).settings;
            if (effective.providerInstances[sourceRun.providerInstanceId]?.enabled === false) {
              return yield* new InlineQuestionError({
                message: "This provider is disabled for the project.",
              });
            }
          }
          if (!adapter.supportsEphemeralFork) {
            return yield* new InlineQuestionError({
              message: "Temporary questions currently support Codex and Claude sessions.",
            });
          }
          const modelSelection = sourceRun.modelSelection;
          const runtimePolicy = yield* policies.resolve({ thread: source.thread, modelSelection });
          const threadId = yield* ids.allocate.thread({ projectId: source.thread.projectId });
          const providerSessionId = yield* ids.allocate.providerSession({
            threadId,
            providerInstanceId: modelSelection.instanceId,
          });
          // Bypass the durable session manager: no child thread, event, or question enters the log.
          const runtime = yield* adapter.openSession({
            threadId,
            providerSessionId,
            modelSelection,
            runtimePolicy,
            ephemeral: true,
            ephemeralSourceThreadId: source.thread.id,
          });
          const subscription = runtime.subscribeEvents ? yield* runtime.subscribeEvents : null;
          if (subscription) yield* Effect.addFinalizer(() => subscription.close);
          const providerThread = yield* runtime.forkThread({
            sourceProviderThread,
            sourceProviderTurns: source.providerTurns,
            providerTurnId: sourceTurn.id,
            targetThreadId: threadId,
            modelSelection,
            runtimePolicy,
          });
          const runId = ids.derive.run({ threadId, ordinal: 1 });
          yield* runtime.startTurn({
            appThread: {
              ...source.thread,
              id: threadId,
              activeProviderThreadId: providerThread.id,
              runtimeMode: runtimePolicy.runtimeMode,
              interactionMode: runtimePolicy.interactionMode,
            },
            threadId,
            runId,
            runOrdinal: 1,
            providerTurnOrdinal: sourceTurn.ordinal + 1,
            nativeThreadHasTurns: true,
            attemptId: ids.derive.runAttempt({ runId, attemptOrdinal: 1 }),
            rootNodeId: ids.derive.rootNode({ runId }),
            providerThread,
            modelSelection,
            runtimePolicy,
            message: {
              messageId: yield* ids.allocate.message({ threadId, ordinal: 1 }),
              text: [
                "Answer this one-off question using the existing conversation. Be concise. Do not use tools, change files, or perform any actions.",
                "Selected passage (quoted context):",
                question.selection,
                "Question:",
                question.question,
              ].join("\n\n"),
              attachments: [],
              createdBy: "user",
              creationSource: "web",
            },
          });
          const messages = new Map<string, string>();
          let completed = false;
          return (subscription?.events ?? runtime.events).pipe(
            Stream.filter(
              (event) =>
                (event.type === "message.updated" &&
                  event.message.threadId === threadId &&
                  event.message.role === "assistant") ||
                event.type === "runtime_request.updated" ||
                (event.type === "turn.terminal" && event.providerThreadId === providerThread.id),
            ),
            Stream.takeUntil((event) => event.type === "turn.terminal"),
            Stream.mapEffect((event) =>
              Effect.gen(function* () {
                if (event.type === "runtime_request.updated") {
                  return yield* new InlineQuestionError({
                    message:
                      "This question requires an interactive tool. Ask it in the main conversation instead.",
                  });
                }
                if (event.type === "turn.terminal" && event.status !== "completed") {
                  return yield* new InlineQuestionError({
                    message: event.failure?.message ?? "The temporary question was interrupted.",
                  });
                }
                if (event.type === "message.updated")
                  messages.set(event.message.id, event.message.text);
                if (event.type === "turn.terminal") completed = true;
                return {
                  text: Array.from(messages.values()).join("\n\n"),
                  status:
                    event.type === "turn.terminal"
                      ? ("completed" as const)
                      : ("answering" as const),
                };
              }),
            ),
            Stream.concat(
              Stream.fromEffect(
                Effect.suspend(() =>
                  completed
                    ? Effect.void
                    : new InlineQuestionError({
                        message: "The temporary session ended before answering. Try again.",
                      }),
                ),
              ).pipe(Stream.drain),
            ),
          );
        }),
      ).pipe(
        Stream.mapError((error) =>
          Schema.is(InlineQuestionError)(error)
            ? error
            : new InlineQuestionError({ message: error.message }),
        ),
      ),
  });
});

export const layer = Layer.effect(InlineQuestionService, make);
