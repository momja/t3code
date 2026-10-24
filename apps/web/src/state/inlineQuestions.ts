import { WS_METHODS } from "@t3tools/contracts";
import { createEnvironmentRpcStreamCommand } from "@t3tools/client-runtime/state/runtime";
import { connectionAtomRuntime } from "../connection/runtime";

export const inlineQuestions = createEnvironmentRpcStreamCommand(connectionAtomRuntime, {
  label: "environment-data:inline-question",
  tag: WS_METHODS.inlineQuestionAsk,
});
