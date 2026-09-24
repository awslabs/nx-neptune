import { useEffect, useRef, useState } from "react";
import { Sparkles, Send, Bot, User, X, Maximize2, Minimize2, CheckCircle, ArrowUpRight, Play, Square, Trash2 } from "lucide-react";
import { clsx } from "clsx";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { useAssistant } from "./context";

// Floating launcher: a minimized/collapsed version of the assistant drawer's
// header, pinned to the top-right above all page chrome. Opens the drawer;
// hidden while the drawer itself is open (the drawer occupies that corner).
export function AssistantLauncher() {
  const { open, setOpen } = useAssistant();
  if (open) return null;
  return (
    <button
      onClick={() => setOpen(true)}
      title="AI Assistant"
      className="fixed right-4 top-5 z-50 inline-flex items-center gap-2 rounded-full bg-purple-800 px-4 py-2 text-sm font-semibold text-white shadow-lg transition-colors hover:bg-purple-900"
    >
      <Sparkles className="h-4 w-4" />
      AI Assistant
    </button>
  );
}

// Global, page-aware AI assistant. Mounted once in App (spec §8.2 decision 2):
// a right-docked drawer that persists across route changes and reads/writes the
// active page through the PageBridge context.
export function AssistantDrawer() {
  const {
    open,
    expanded,
    setOpen,
    setExpanded,
    bedrockModel,
    setBedrockModel,
    chat,
    thinking,
    sendChat,
    clearChat,
    runJump,
    runChatAction,
  } = useAssistant();

  const [input, setInput] = useState("");
  const scrollRef = useRef<HTMLDivElement>(null);

  // Keep the transcript pinned to the latest message.
  useEffect(() => {
    scrollRef.current?.scrollTo({ top: scrollRef.current.scrollHeight });
  }, [chat, thinking, open]);

  if (!open) return null;

  function submit() {
    const text = input.trim();
    if (!text) return;
    sendChat(text);
    setInput("");
  }

  return (
    <aside
      className={clsx(
        "fixed right-0 top-0 z-40 flex h-screen flex-col border-l border-purple-200 bg-purple-50/40 shadow-xl transition-all",
        expanded ? "w-[560px]" : "w-[380px]",
      )}
    >
      <div className="flex shrink-0 items-center justify-between border-b border-purple-200 bg-purple-800 px-4 py-2.5">
        <div className="flex items-center gap-2 text-white">
          <Sparkles className="h-4 w-4" />
          <span className="text-sm font-semibold">AI Assistant</span>
        </div>
        <div className="flex items-center gap-2">
          <select
            value={bedrockModel}
            onChange={(e) => setBedrockModel(e.target.value)}
            className="rounded border border-purple-400 bg-purple-700 px-2 py-1 text-xs text-white focus:outline-none"
            title="Bedrock model"
          >
            <option value="us.anthropic.claude-sonnet-4-5-20250929-v1:0">Claude Sonnet 4.5</option>
            <option value="us.anthropic.claude-opus-4-1-20250805-v1:0">Claude Opus 4.1</option>
            <option value="us.anthropic.claude-haiku-4-5-20251001-v1:0">Claude Haiku 4.5</option>
          </select>
          <button
            onClick={clearChat}
            disabled={thinking || chat.length <= 1}
            className="text-purple-100 hover:text-white disabled:cursor-not-allowed disabled:opacity-40"
            title="Clear conversation"
          >
            <Trash2 className="h-4 w-4" />
          </button>
          <button
            onClick={() => setExpanded(!expanded)}
            className="text-purple-100 hover:text-white"
            title={expanded ? "Shrink" : "Expand"}
          >
            {expanded ? <Minimize2 className="h-4 w-4" /> : <Maximize2 className="h-4 w-4" />}
          </button>
          <button onClick={() => setOpen(false)} className="text-purple-100 hover:text-white" title="Close">
            <X className="h-4 w-4" />
          </button>
        </div>
      </div>

      <div ref={scrollRef} className="flex-1 min-h-0 space-y-3 overflow-y-auto px-4 py-3">
        {chat.map((m, i) => (
          <div key={i} className={clsx("flex gap-2", m.role === "user" && "flex-row-reverse")}>
            <div
              className={clsx(
                "mt-0.5 flex h-6 w-6 shrink-0 items-center justify-center rounded-full",
                m.role === "user" ? "bg-gray-200 text-gray-600" : "bg-purple-800 text-white",
              )}
            >
              {m.role === "user" ? <User className="h-3.5 w-3.5" /> : <Bot className="h-3.5 w-3.5" />}
            </div>
            <div
              className={clsx(
                "max-w-[80%] rounded-lg px-3 py-2 text-sm",
                m.role === "user"
                  ? "bg-blue-600 text-white"
                  : "border border-purple-200 bg-white text-gray-700",
              )}
            >
              <div className="overflow-x-auto [&_p]:my-0 [&_p+p]:mt-2 [&_code]:rounded [&_code]:bg-gray-100 [&_code]:px-1 [&_code]:py-0.5 [&_code]:font-mono [&_code]:text-[0.85em] [&_ul]:my-1 [&_ul]:list-disc [&_ul]:pl-4 [&_ol]:my-1 [&_ol]:list-decimal [&_ol]:pl-4 [&_a]:underline [&_table]:my-1 [&_table]:w-full [&_table]:border-collapse [&_table]:text-xs [&_th]:border [&_th]:border-gray-300 [&_th]:bg-gray-100 [&_th]:px-2 [&_th]:py-1 [&_th]:text-left [&_td]:border [&_td]:border-gray-300 [&_td]:px-2 [&_td]:py-1">
                <ReactMarkdown remarkPlugins={[remarkGfm]}>{m.text}</ReactMarkdown>
              </div>
              {m.applied && m.applied.length > 0 && (
                <div className="mt-2 flex flex-wrap gap-1">
                  {m.applied.map((f) => (
                    <span
                      key={f}
                      className="inline-flex items-center gap-1 rounded-full bg-purple-100 px-2 py-0.5 text-[11px] font-medium text-purple-800"
                    >
                      <CheckCircle className="h-3 w-3" /> {f}
                    </span>
                  ))}
                </div>
              )}
              {m.jumps && m.jumps.length > 0 && (
                <div className="mt-2 flex flex-wrap gap-1.5">
                  {m.jumps.map((j, ji) => (
                    <button
                      key={ji}
                      onClick={() => runJump(j)}
                      className="inline-flex items-center gap-1 rounded-md border border-purple-300 bg-white px-2.5 py-1 text-xs font-medium text-purple-800 shadow-sm hover:bg-purple-50"
                    >
                      <ArrowUpRight className="h-3.5 w-3.5" /> {j.label}
                    </button>
                  ))}
                </div>
              )}
              {m.actions && m.actions.length > 0 && (
                <div className="mt-2 flex flex-wrap gap-1.5">
                  {m.actions.map((a, ai) => {
                    const disabled = a.kind === "page-action" && a.enabled === false;
                    const Icon =
                      a.kind === "graph-action"
                        ? a.graphAction === "delete"
                          ? Trash2
                          : Square
                        : Play;
                    return (
                      <button
                        key={ai}
                        disabled={disabled}
                        onClick={() => runChatAction(a)}
                        className={clsx(
                          "inline-flex items-center gap-1 rounded-md border px-2.5 py-1 text-xs font-medium shadow-sm disabled:cursor-not-allowed disabled:opacity-40",
                          a.destructive
                            ? "border-red-300 bg-white text-red-700 hover:bg-red-50"
                            : "border-purple-300 bg-white text-purple-800 hover:bg-purple-50",
                        )}
                      >
                        <Icon className="h-3.5 w-3.5" /> {a.label}
                      </button>
                    );
                  })}
                </div>
              )}
            </div>
          </div>
        ))}
        {thinking && (
          <div className="flex gap-2">
            <div className="mt-0.5 flex h-6 w-6 shrink-0 items-center justify-center rounded-full bg-purple-800 text-white">
              <Bot className="h-3.5 w-3.5" />
            </div>
            <div className="rounded-lg border border-purple-200 bg-white px-3 py-2 text-sm text-gray-400">
              Thinking…
            </div>
          </div>
        )}
      </div>

      <div className="flex shrink-0 items-center gap-2 border-t border-purple-200 px-4 py-3">
        <input
          className="flex-1 rounded-md border border-gray-300 px-3 py-2 text-sm shadow-sm focus:border-purple-500 focus:ring-1 focus:ring-purple-500"
          placeholder='e.g. "get the page rank from all malware in my database"'
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => { if (e.key === "Enter") submit(); }}
        />
        <button
          onClick={submit}
          className="inline-flex items-center gap-2 rounded-md bg-purple-800 px-4 py-2 text-sm font-medium text-white hover:bg-purple-900"
        >
          <Send className="h-4 w-4" /> Send
        </button>
      </div>
    </aside>
  );
}
