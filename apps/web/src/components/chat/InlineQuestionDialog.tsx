import { useAtomValue } from "@effect/atom-react";
import { INLINE_QUESTION_MAX_LENGTH, type AssistantCitation } from "@t3tools/contracts";
import { useState } from "react";
import { MessageCircleQuestionIcon } from "lucide-react";
import { inlineQuestions } from "~/state/inlineQuestions";
import { useEnvironmentQuery } from "~/state/query";
import ChatMarkdown from "../ChatMarkdown";
import { Button } from "../ui/button";
import {
  Dialog,
  DialogPopup,
  DialogTitle,
  DialogDescription,
  DialogHeader,
  DialogPanel,
  DialogFooter,
} from "../ui/dialog";

export function InlineQuestionDialog({
  citation,
  onClose,
}: {
  citation: AssistantCitation;
  onClose: () => void;
}) {
  const [question, setQuestion] = useState("");
  const [request, setRequest] = useState<ReturnType<typeof inlineQuestions.stream> | null>(null);
  const allowed = useAtomValue(inlineQuestions.permissionAtom(citation.environmentId));
  const response = useEnvironmentQuery(request);
  const busy = request !== null && response.data?.status !== "completed" && response.error === null;
  const submit = () => {
    if (busy || !allowed || !question.trim() || question.length > INLINE_QUESTION_MAX_LENGTH)
      return;
    setRequest(
      inlineQuestions.stream({
        environmentId: citation.environmentId,
        input: {
          threadId: citation.threadId,
          question,
          selection: citation.text,
        },
      }),
    );
  };
  return (
    <Dialog
      open
      onOpenChange={(open) => {
        if (!open) onClose();
      }}
    >
      <DialogPopup className="w-full">
        <DialogHeader>
          <div className="flex items-center gap-2 pr-8">
            <MessageCircleQuestionIcon className="size-4 text-muted-foreground" aria-hidden />
            <DialogTitle>Ask a quick question</DialogTitle>
          </div>
          <DialogDescription>Only shown here. Close to forget.</DialogDescription>
        </DialogHeader>
        <form
          className="flex min-h-0 flex-col"
          onSubmit={(event) => {
            event.preventDefault();
            submit();
          }}
        >
          <DialogPanel>
            <blockquote className="max-h-28 overflow-y-auto border-l-2 border-border pl-3 text-sm text-muted-foreground whitespace-pre-wrap">
              {citation.text}
            </blockquote>
            <textarea
              autoFocus
              aria-label="Question about selected text"
              placeholder="What would you like to know?"
              rows={2}
              maxLength={INLINE_QUESTION_MAX_LENGTH}
              className="block max-h-40 min-h-16 w-full resize-none bg-transparent px-1 py-1.5 text-base outline-none placeholder:text-muted-foreground sm:text-sm"
              value={question}
              disabled={busy}
              onChange={(event) => setQuestion(event.currentTarget.value)}
              onKeyDown={(event) => {
                event.stopPropagation();
                if (event.key === "Escape" && !event.nativeEvent.isComposing) {
                  event.preventDefault();
                  onClose();
                  return;
                }
                if (
                  event.key === "Enter" &&
                  !event.shiftKey &&
                  !event.nativeEvent.isComposing &&
                  event.keyCode !== 229
                ) {
                  event.preventDefault();
                  submit();
                }
              }}
            />
            {response.data?.text ? (
              <div
                className="mt-4 max-h-[45vh] overflow-y-auto border-t border-border pt-4"
                aria-label="Answer"
              >
                <ChatMarkdown
                  text={response.data.text}
                  cwd={undefined}
                  environmentId={citation.environmentId}
                  isStreaming={busy}
                />
              </div>
            ) : null}
            {busy ? (
              <p role="status" className="mt-3 text-xs text-muted-foreground">
                Thinking…
              </p>
            ) : null}
            {response.error ? (
              <p role="alert" className="mt-3 text-sm text-destructive">
                {response.error}
              </p>
            ) : null}
            {!allowed ? (
              <p role="status" className="mt-3 text-xs text-muted-foreground">
                This connection cannot ask questions.
              </p>
            ) : null}
          </DialogPanel>
          <DialogFooter variant="bare">
            <Button variant="outline" size="sm" onClick={onClose}>
              Close
            </Button>
            <Button type="submit" size="sm" disabled={busy || !allowed || !question.trim()}>
              Ask
            </Button>
          </DialogFooter>
        </form>
      </DialogPopup>
    </Dialog>
  );
}
