import * as Schema from "effect/Schema";
import { ThreadId, TrimmedNonEmptyString } from "./baseSchemas.ts";

export const INLINE_QUESTION_MAX_LENGTH = 8_000;
const INLINE_QUESTION_SELECTION_MAX_LENGTH = 8_000;

export const InlineQuestionInput = Schema.Struct({
  threadId: ThreadId,
  question: TrimmedNonEmptyString.check(Schema.isMaxLength(INLINE_QUESTION_MAX_LENGTH)),
  selection: Schema.String.check(Schema.isMaxLength(INLINE_QUESTION_SELECTION_MAX_LENGTH)),
});
export type InlineQuestionInput = typeof InlineQuestionInput.Type;

export const InlineQuestionEvent = Schema.Struct({
  text: Schema.String,
  status: Schema.Literals(["answering", "completed"]),
});
export type InlineQuestionEvent = typeof InlineQuestionEvent.Type;

export class InlineQuestionError extends Schema.TaggedError<InlineQuestionError>()(
  "InlineQuestionError",
  { message: Schema.String },
) {}
