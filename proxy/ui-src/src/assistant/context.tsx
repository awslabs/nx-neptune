import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { useNavigate } from "react-router";
import { cannedRespond } from "./mock";
import { remoteRespond } from "./remote";
import { assistantApi } from "../api";

// --- Types --------------------------------------------------------------
// A single page action the assistant can surface as an inline chat button.
// Groups 4/5 render these; Group 1 just carries the shape.
export interface ActionSpec {
  label: string;
  run: () => void | Promise<void>;
  enabled?: boolean;
  destructive?: boolean;
}

// A Neptune graph the assistant can act on (Stop/Delete Instance). Pages that
// list graphs register these so Groups 4/5 can offer a per-graph action picker.
export interface GraphTarget {
  id: string;
  name: string;
  status: string;
  // Actions currently available on this graph (from graphActions.getActions),
  // e.g. ["stop", "start", "delete"].
  actions: string[];
}

// What a page registers with the assistant while it is mounted. The assistant
// reads/writes the *current* page through this bridge (see spec §8.2).
export interface PageBridge {
  page: string;
  fields?: Record<string, unknown>;
  // Field setters, keyed by field name. Accept a value or a React-style updater;
  // the assistant mock calls these to fill the current page.
  setters?: Record<string, (value: any) => void>;
  // Page-level singleton actions (e.g. Import's execute / validateQuery / preview).
  actions?: Record<string, ActionSpec>;
  // Per-graph action targets (e.g. Stop/Delete Instance on Graphs / Projections).
  graphTargets?: GraphTarget[];
  // Runs a graph action; the owning page handles confirmation + refresh.
  runGraphAction?: (graphId: string, action: string) => void | Promise<void>;
  // Runs a single openCypher query against the page's graph, returning the
  // result (or error) so the assistant can echo it into the transcript. Its
  // presence tells the backend the page can run queries (see serializePageContext).
  runGraphQuery?: (cypher: string) => Promise<{ result?: unknown; error?: string }>;
  // Persist the just-applied import as a projection (creating one if none
  // exists) after the assistant fills the form. The applied values are passed
  // in directly because the setters above are async — React state has not
  // re-rendered yet when this is called (see spec §9.5).
  persistImport?: (data: ImportPersistData) => void | Promise<void>;
  // Current project context from the URL (null when none), used by jumps.
  jumpContext?: { projectId: string | null };
}

// The import fields the assistant just applied, forwarded to persistImport so
// the created/updated projection matches the proposal exactly. Omitted fields
// are left untouched (a follow-up turn editing only graph queries, say, must
// not clobber previously saved SQL).
export interface ImportPersistData {
  catalog?: string;
  database?: string;
  bucket?: string;
  graphName?: string;
  nodeQueries?: { sql: string }[];
  edgeQueries?: { sql: string }[];
}

// A cross-page navigation the assistant can offer as an inline chat button
// (spec §8.2 decision 7). New Import / New Project are always available;
// open-projections is a jump to a single, already-resolved project's graphs
// (the mock suggests one project by name — see detectJumps).
export type JumpAction =
  | { kind: "new-import"; label: string; projectId: string | null }
  | { kind: "new-project"; label: string }
  | { kind: "open-projections"; label: string; projectId: string };

// A suggested page action rendered inline in the transcript (spec §8.2
// decisions 5/6/8). Descriptors reference the page + action by key/target so the
// click runs against the *currently mounted* bridge — never a stale page.
export type ChatAction =
  | {
      kind: "page-action";
      page: string;
      actionKey: string;
      label: string;
      enabled?: boolean;
      destructive?: boolean;
    }
  | {
      kind: "graph-action";
      page: string;
      graphId: string;
      graphAction: string;
      label: string;
      destructive?: boolean;
    }
  | {
      kind: "run-query";
      page: string;
      query: string;
      label: string;
      destructive?: boolean;
    };

export interface ChatMessage {
  role: "assistant" | "user";
  text: string;
  applied?: string[];
  // Inline jump buttons attached to this message (spec §8.2 decision 8).
  jumps?: JumpAction[];
  // Inline page-action buttons attached to this message (spec §8.2 decision 8).
  actions?: ChatAction[];
}

interface AssistantContextValue {
  // Drawer UI state
  open: boolean;
  expanded: boolean;
  toggle: () => void;
  setOpen: (open: boolean) => void;
  setExpanded: (expanded: boolean) => void;
  // Model + conversation
  bedrockModel: string;
  setBedrockModel: (model: string) => void;
  chat: ChatMessage[];
  thinking: boolean;
  sendChat: (text: string) => void;
  // Reset the conversation: clears the transcript back to the greeting and
  // drops the server-side session so the next message starts fresh context.
  clearChat: () => void;
  // Perform an inline jump button (navigates, or expands into a project picker).
  runJump: (jump: JumpAction) => void;
  // Run an inline page/graph action against the currently mounted page bridge.
  runChatAction: (action: ChatAction) => void;
  // Active page bridge (page identity drives re-render; latest reg lives in a ref)
  bridgePage: string | null;
  getBridge: () => PageBridge | null;
  registerBridge: (reg: PageBridge) => void;
  clearBridge: (page: string) => void;
}

const AssistantContext = createContext<AssistantContextValue | null>(null);

const GREETING: ChatMessage = {
  role: "assistant",
  text:
    "Hi! Tell me what you want to do and I'll help you set it up on the current page — " +
    "or jump you to the right one. On the Import page I can fill in the catalog, database, " +
    "staging bucket, graph name, and the node/edge SQL, and suggest openCypher to run after import.",
};

export function AssistantProvider({ children }: { children: ReactNode }) {
  const [open, setOpen] = useState(false);
  const [expanded, setExpanded] = useState(false);
  const [bedrockModel, setBedrockModel] = useState("us.anthropic.claude-sonnet-4-5-20250929-v1:0");
  const [chat, setChat] = useState<ChatMessage[]>([GREETING]);
  const [thinking, setThinking] = useState(false);
  const navigate = useNavigate();

  // The live registration is kept in a ref so pages can refresh it every render
  // (e.g. changing field values / enabled flags) without causing re-render loops.
  // Only the *page identity* is state, so the drawer re-renders when the active
  // page changes.
  const bridgeRef = useRef<PageBridge | null>(null);
  const [bridgePage, setBridgePage] = useState<string | null>(null);

  // Assistant session id (holds server-side history + discovery cache). Minted
  // lazily on the first message; reused for the rest of the conversation.
  const sessionIdRef = useRef<string | null>(null);

  const registerBridge = useCallback((reg: PageBridge) => {
    bridgeRef.current = reg;
    setBridgePage((prev) => (prev === reg.page ? prev : reg.page));
  }, []);

  const clearBridge = useCallback((page: string) => {
    if (bridgeRef.current?.page === page) {
      bridgeRef.current = null;
      setBridgePage(null);
    }
  }, []);

  const getBridge = useCallback(() => bridgeRef.current, []);

  const toggle = useCallback(() => setOpen((v) => !v), []);

  // Reset the conversation. Dropping the session id means the next sendChat
  // mints a brand-new server session (fresh history + discovery cache), so the
  // old LLM context is fully abandoned — the in-memory server session is simply
  // no longer referenced.
  const clearChat = useCallback(() => {
    sessionIdRef.current = null;
    setThinking(false);
    setChat([GREETING]);
  }, []);

  // Send one turn to the assistant agent backend (spec §9). The active page is
  // serialized into a PageContext and sent with the message; the reply's import
  // proposal is applied through the page bridge setters and its jumps/actions
  // become inline buttons (see assistant/remote.ts). If the backend is
  // unavailable (offline, or strands-agents not installed), we fall back to the
  // deterministic offline mock so the prototype still works.
  const sendChat = useCallback(
    (raw: string) => {
      const text = raw.trim();
      if (!text) return;
      setChat((prev) => [...prev, { role: "user", text }]);
      setThinking(true);
      void (async () => {
        const bridge = bridgeRef.current;
        try {
          if (!sessionIdRef.current) {
            const { session_id } = await assistantApi.session();
            sessionIdRef.current = session_id;
          }
          const reply = await remoteRespond(
            text,
            bridge,
            sessionIdRef.current,
            bedrockModel,
          );
          setChat((prev) => [...prev, reply]);
        } catch {
          const reply = await cannedRespond(text, bridge);
          setChat((prev) => [...prev, reply]);
        } finally {
          setThinking(false);
        }
      })();
    },
    [bedrockModel],
  );

  // Inline jump buttons (spec §8.2 decision 7). All targets are resolved before
  // the button is rendered (the mock suggests a single project by name), so a
  // click just navigates.
  const runJump = useCallback(
    (jump: JumpAction) => {
      switch (jump.kind) {
        case "new-import":
          navigate(`/import?${jump.projectId ? `project=${jump.projectId}&` : ""}t=${Date.now()}`);
          break;
        case "new-project":
          navigate("/projects");
          break;
        case "open-projections":
          navigate(`/projections?project=${jump.projectId}`);
          break;
      }
    },
    [navigate],
  );

  // Run a suggested action against the *currently mounted* page (spec §8.2:
  // never write to an unmounted page). Destructive graph actions reuse the
  // owning page's confirm() + refresh via runGraphAction. Results/errors are
  // surfaced back into the transcript.
  const runChatAction = useCallback((action: ChatAction) => {
    const bridge = bridgeRef.current;
    const note = (text: string) =>
      setChat((prev) => [...prev, { role: "assistant", text }]);

    if (!bridge || bridge.page !== action.page) {
      note(`That action isn't available here anymore — go back to the ${action.page} page and try again.`);
      return;
    }

    void (async () => {
      try {
        if (action.kind === "page-action") {
          const spec = bridge.actions?.[action.actionKey];
          if (!spec) return note(`“${action.label}” isn't available on this page.`);
          if (spec.enabled === false) return note(`“${spec.label}” is currently disabled.`);
          await spec.run();
          note(`Ran “${spec.label}”.`);
        } else if (action.kind === "run-query") {
          if (!bridge.runGraphQuery) return note("Running queries isn't available on this page.");
          const { result, error } = await bridge.runGraphQuery(action.query);
          if (error) return note(`Query failed: ${error}`);
          note(`Ran the query. Results:\n\n\`\`\`json\n${JSON.stringify(result, null, 2)}\n\`\`\``);
        } else {
          if (!bridge.runGraphAction) return note("Graph actions aren't available on this page.");
          await bridge.runGraphAction(action.graphId, action.graphAction);
          note(`Requested ${action.graphAction} on ${action.label.replace(/^(Stop|Delete)\s+/i, "")}.`);
        }
      } catch (e: any) {
        note(`That action failed: ${e?.message ?? e}`);
      }
    })();
  }, []);

  const value: AssistantContextValue = {
    open,
    expanded,
    toggle,
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
    bridgePage,
    getBridge,
    registerBridge,
    clearBridge,
  };

  return <AssistantContext.Provider value={value}>{children}</AssistantContext.Provider>;
}

export function useAssistant(): AssistantContextValue {
  const ctx = useContext(AssistantContext);
  if (!ctx) throw new Error("useAssistant must be used within an AssistantProvider");
  return ctx;
}

/**
 * Register the current page with the assistant. Call once per page component;
 * the registration is refreshed on every render (cheap — only page changes
 * trigger a provider re-render) and cleared when the page unmounts.
 */
export function usePageBridge(reg: PageBridge) {
  const { registerBridge, clearBridge } = useAssistant();

  // Refresh the latest registration (fields / enabled flags) every render.
  useEffect(() => {
    registerBridge(reg);
  });

  // Unregister when the page unmounts or its identity changes.
  useEffect(() => {
    return () => clearBridge(reg.page);
  }, [reg.page, clearBridge]);
}
